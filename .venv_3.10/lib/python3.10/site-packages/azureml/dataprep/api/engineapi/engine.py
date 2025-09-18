# Copyright (c) Microsoft Corporation. All rights reserved.
"""Launch and exchange messages with the engine."""
import atexit
import json
import os
import subprocess
import sys
from abc import ABC, abstractmethod
from queue import Queue
from threading import Thread, Event, RLock
from typing import Callable
from dotnetcore2 import runtime
from .typedefinitions import CustomEncoder
from ._termination import IsOOMKill, check_process_oom_killed
from ..errorhandlers import raise_engine_error, ExecutionError
import atexit

from .._loggerfactory import session_id, instrumentation_key, _LoggerFactory, verbosity, HBI_MODE, log_directory


log = _LoggerFactory.get_logger('dprep.engine')

use_multithread_channel = True


class CancellationToken:
    def __init__(self):
        self._callbacks = []
        self.is_canceled = False

    def register(self, callback):
        self._callbacks.append(callback)

    def cancel(self):
        self.is_canceled = True
        for cb in self._callbacks:
            cb()


class AbstractMessageChannel(ABC):
    """Base class for MessageChannel"""

    ENGINE_TERMINATED = b'ENGINE_TERMINATED'
    ENGINE_TERMINATED_OOM = b'ENGINE_TERMINATED_OOM'
    ENGINE_TERMINATED_OOM_MAYBE = b'ENGINE_TERMINATED_OOM_MAYBE'

    def __init__(self, process_opener: Callable[[], subprocess.Popen]):
        self._process_opener = process_opener
        self._last_message_id = 0
        self._process = None
        self._engine_wait_thread = None
        self._response_thread = None
        self._response_queue = None
        self._relaunch_callback = None
        self._closing = False
        self._process_lock = RLock()
        self._ensure_process(caller='AbstractMessageChannel.__init__')

    @abstractmethod
    def on_relaunch(self, callback: Callable[[], None]):
        pass

    @abstractmethod
    def send_message(self, op_code: str, message: object, cancellation_token: CancellationToken = None) -> object:
        pass

    @abstractmethod
    def _ensure_process(self, caller=None):
        pass

    def close(self):
        """Close the underlying process."""
        _LoggerFactory.trace(log, '{}.close'.format(type(self).__name__), {'engine_pid': self._process.pid})
        self._closing = True
        self.send_message('Quit', {}, None)
        with self._process_lock:
            self._process.terminate()
            try:
                self._process.wait(10)
            except subprocess.TimeoutExpired:
                self._process.kill()

    def _write_line(self, line: str):
        with self._process_lock:
            self._ensure_process(caller='AbstractMessageChannel._write_line')
            self._process.stdin.write((line + '\n').encode())
            self._process.stdin.flush()

    def _wait_on_engine_death(self, process, queue):
        # wait for unexpected Engine termination and add sentinel to queue indicating as such.
        returncode = process.wait()
        if returncode != 0 and not self._closing:
            log.error('Engine process terminated with returncode={}'.format(returncode))
        is_oom_killed = check_process_oom_killed(process)
        if is_oom_killed == IsOOMKill.YES:
            queue.put(self.ENGINE_TERMINATED_OOM)
        elif is_oom_killed == IsOOMKill.MAYBE:
            queue.put(self.ENGINE_TERMINATED_OOM_MAYBE)
        else:
            queue.put(self.ENGINE_TERMINATED)

    def _renew_wait_thread(self):
        self._engine_wait_thread = Thread(target=self._wait_on_engine_death, args=(self._process, self._response_queue))
        self._engine_wait_thread.daemon = True # thread dies with the program
        self._engine_wait_thread.start()

    def _renew_response_thread(self):
        self._response_queue = Queue()
        self._response_thread = Thread(target=self._enqueue_responses, args=(self._process.stdout, self._response_queue))
        self._response_thread.daemon = True # thread dies with the program
        self._response_thread.start()

    def _enqueue_responses(self, out, queue):
        """ _ensure_process starts this method in a separate thread so readline doesn't block the main thread.
        """
        for line in iter(out.readline, b''):
            queue.put(line)
        out.close()
        
    def _read_response(self, caller=None) -> dict:
        string = None
        error = None
        while string is None:
            line = self._response_queue.get() # blocks
            if line is self.ENGINE_TERMINATED:
                break
            if line is self.ENGINE_TERMINATED_OOM_MAYBE:
                error = MemoryError(_format_with_session_id(
                    'Engine process terminated. This is most likely due to system running out of memory. '
                    'Please retry with increased memory.'))
                break
            if line is self.ENGINE_TERMINATED_OOM:
                error = MemoryError(_format_with_session_id(
                    'Engine process terminated due to system running out of memory. '
                    'Please retry with increased memory.'))
                break
            string = line.decode()
            if len(string) == 0 or string[0] != '{':
                log.error('Engine sent unexpected response: {}'.format(string))
                string = None
        if string is None:
            # If message channel is currently closing Engine process termination is expected and should be ignored.
            if self._closing:
                return
            error = error or RuntimeError(_format_with_session_id(
                'Engine process terminated. Please try running again.'))
            log.error(repr(error))
            raise error

        parsed = None
        try:
            parsed = json.loads(string)
        finally:
            if parsed is None:  # Exception is being thrown
                print('Line read from engine could not be parsed as JSON. Line:')
                try:
                    print(string)
                except UnicodeEncodeError:
                    print(bytes(string, 'utf-8'))
        return parsed


class SingleThreadMessageChannel(AbstractMessageChannel):
    """Single threaded channel for JSON messages to the local engine process."""
    def __init__(self, process_opener: Callable[[], subprocess.Popen]):
        super().__init__(process_opener)
        self._lock = RLock()
        _LoggerFactory.trace(log, 'SingleThreadMessageChannel_create')
        self._ensure_process(caller='SingleThreadMessageChannel.__init__')

    def on_relaunch(self, callback: Callable[[], None]):
        self._relaunch_callback = callback

    def send_message(self, op_code: str, message: object, cancellation_token: CancellationToken = None) -> object:
        self._lock.acquire()
        try:
            """Send a message to the engine, and wait for its response."""
            self._last_message_id += 1
            message_id = self._last_message_id
            self._write_line(json.dumps({
                'messageId': message_id,
                'opCode': op_code,
                'data': message
            }, cls=CustomEncoder))

            while True:
                response = self._read_response(caller='SingleThreadMessageChannel.send_message')
                if 'error' in response:
                    raise_engine_error(response['error'])
                elif response.get('id') == message_id:
                    return response['result']
                else:
                    log.error('Unexpected response ID for message.')
        finally:
            self._lock.release()

    def _ensure_process(self, caller=None):
        if self._process is None or self._process.poll() is not None:
            log.debug('Starting SingleThreadMessageChannel because {}'.format(caller))
            self._process = self._process_opener()
            self._renew_response_thread()
            self._renew_wait_thread()
            _LoggerFactory.trace(log, 'SingleThreadMessageChannel_create_engine',
                                 {'engine_pid': self._process.pid, 'caller': caller})
            if self._relaunch_callback is not None:
                self._relaunch_callback()


class MultiThreadMessageChannel(AbstractMessageChannel):
    """Channel for JSON messages to the local engine process."""
    def __init__(self, process_opener: Callable[[], subprocess.Popen]):
        self._messages_lock = RLock()
        self._pending_messages = {}
        self._was_relaunched = False
        self._relaunch_lock = RLock()
        self._relaunch_callback = None
        super().__init__(process_opener)
        _LoggerFactory.trace(log, 'MultiThreadMessageChannel_create')

        def process_responses():
            while True and not self._closing:
                try:
                    response = self._read_response(caller='MultiThreadMessageChannel.process_responses')
                    with self._messages_lock:
                        pending_message = self._pending_messages[response['id']]
                        pending_message['response'] = response
                        pending_message['event'].set()
                except Exception as e:
                    log.info('[MultiThreadMessageChannel.process_responses()] Exception while reading response: {}'.format(e))
                    with self._messages_lock:
                        for pending_message in self._pending_messages.values():
                            pending_message['error'] = e
                            pending_message['event'].set()

        self._responses_thread = Thread(target=process_responses, daemon=True)
        self._responses_thread.start()
        atexit.register(self._destroy)

    def on_relaunch(self, callback: Callable[[], None]):
        self._relaunch_callback = callback

    def send_message(self, op_code: str, message: object, cancellation_token: CancellationToken = None) -> object:
        """Send a message to the engine, and wait for its response."""
        if self._relaunch_callback is not None and self._was_relaunched:
            with self._relaunch_lock:
                self._was_relaunched = False
                self._relaunch_callback()

        self._last_message_id += 1
        message_id = self._last_message_id
        is_done = False

        def cancel_message():
            if is_done:
                return

            self.send_message('CancelMessage', {'idToCancel': message_id})

        if cancellation_token is not None:
            cancellation_token.register(cancel_message)

        event = Event()
        with self._messages_lock:
            self._pending_messages[message_id] = {'event': event, 'response': None}
        self._write_line(json.dumps({
            'messageId': message_id,
            'opCode': op_code,
            'data': message
        }, cls=CustomEncoder))

        event.wait()
        with self._messages_lock:
            message = self._pending_messages.pop(message_id, None)
            if message.get('error') is not None:
                raise message['error']
            response = message['response']
            is_done = True

        def cancel_on_error():
            if cancellation_token is not None:
                cancellation_token.cancel()

        if response is None:
            if not self._closing:
                cancel_on_error()
                raise ExecutionError({'errorData': {'errorMessage': 'An unknown error has occurred.'}})
            return None

        if 'error' in response:
            cancel_on_error()
            raise_engine_error(response['error'])
        else:
            return response['result']

    def _ensure_process(self, caller=None):
        if self._process is None or self._process.poll() is not None:
            with self._process_lock:
                if self._process is None or self._process.poll() is not None:
                    log.debug('Starting MultiThreadMessageChannel because {}'.format(caller))
                    self._process = self._process_opener()
                    self._renew_response_thread()
                    self._renew_wait_thread()
                    _LoggerFactory.trace(log, 'MultiThreadMessageChannel_create_engine',
                                         {'engine_pid': self._process.pid, 'caller': caller, 'pendingMessagesCount': len(self._pending_messages) })
                
                    with self._relaunch_lock:
                        self._was_relaunched = True

    def _destroy(self):
        try:
            if self._process is not None:
                self._process.kill()
        except:
            pass


def launch_engine() -> AbstractMessageChannel:
    """Launch the engine process and set up a MessageChannel."""
    engine_args = {
        'debug': 'false',
        'firstLaunch': 'false',
        'sessionId': session_id,
        'invokingPythonPath': sys.executable,
        'invokingPythonWorkingDirectory': os.path.join(os.getcwd(), ''),
        'instrumentationKey': '' if os.environ.get('DISABLE_DPREP_LOGGER') is not None else instrumentation_key,
        'hbiMode': HBI_MODE,
        'verbosity': verbosity,
        'logDirectory': log_directory
    }

    engine_path = _get_engine_path()
    try:
        dependencies_path = runtime.ensure_dependencies()
    except Exception as e:
        _LoggerFactory.trace(log, 'Failed to ensure dependencies' + str(e))
        raise

    dotnet_path = runtime.get_runtime_path()
    engine_cmd = [dotnet_path, engine_path, json.dumps(engine_args)]

    env = os.environ.copy()
    if dependencies_path is not None:
        log.debug('Using deps path: {}'.format(dependencies_path))
        if 'LD_LIBRARY_PATH' in env:
            log.debug('LD_LIBRARY_PATH exists: {}'.format(env['LD_LIBRARY_PATH']))
            env['LD_LIBRARY_PATH'] += ':{}'.format(dependencies_path)
            log.debug('LD_LIBRARY_PATH new: {}'.format(env['LD_LIBRARY_PATH']))
        else:
            env['LD_LIBRARY_PATH'] = dependencies_path
            log.debug('LD_LIBRARY_PATH new: {}'.format(env['LD_LIBRARY_PATH']))

    if sys.platform == 'win32':
        env['PATH'] = env.get('PATH') or ''
    else:
        env['LD_LIBRARY_PATH'] = env.get('LD_LIBRARY_PATH') or ''

    _set_sslcert_path(env)
    # This will be removed once dotnetcore2 can ensure that OS globalization deps (e.g. ICU) are always installed
    _enable_globalization_invariant_if_needed(dotnet_path, env)

    def create_engine_process():
        return subprocess.Popen(
            engine_cmd,
            bufsize=5 * 1024 * 1024,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,  # Hide spurious dotnet errors (Vienna:45714) TODO: collect Errors
            env=env)
    global use_multithread_channel
    if use_multithread_channel:
        channel = MultiThreadMessageChannel(create_engine_process)
    else:
        channel = SingleThreadMessageChannel(create_engine_process)
    atexit.register(channel.close)
    return channel


def use_single_thread_channel():
    global use_multithread_channel
    use_multithread_channel = False


def use_multi_thread_channel():
    global use_multithread_channel
    use_multithread_channel = True


def _get_engine_path():
    return _get_engine_dll_path('Microsoft.DPrep.Execution.EngineHost.dll')


def _get_engine_dll_path(dll_name):
    current_folder = os.path.dirname(os.path.realpath(__file__))
    engine_path = os.path.join(current_folder, 'bin', dll_name)
    return engine_path


def _set_sslcert_path(env):
    if sys.platform in ['win32', 'darwin']:
        return  # no need to set ssl cert path for Windows and macOS

    SSL_CERT_FILE = 'SSL_CERT_FILE'
    if SSL_CERT_FILE in env:
        return  # skip if user has set SSL_CERT_FILE

    try:
        import certifi
        cert_path = os.path.join(os.path.dirname(certifi.__file__), 'cacert.pem')
        if os.path.isfile(cert_path):
            env[SSL_CERT_FILE] = cert_path
    except Exception:
        pass  # keep going since the trusted cert is only missing for some distro of Linux


def _enable_globalization_invariant_if_needed(dotnet_path, env):
    process = subprocess.Popen(
        [dotnet_path, _get_engine_dll_path('Microsoft.DPrep.GlobalizationCheck.dll')],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env)
    err = process.communicate()[1].decode()
    if not err:
        return
    if 'Set the configuration flag System.Globalization.Invariant to true' in err:
        # enable globalization invariant mode
        # https://docs.microsoft.com/en-us/dotnet/core/run-time-config/globalization
        env['DOTNET_SYSTEM_GLOBALIZATION_INVARIANT'] = 'true'
        log.warning(
            'Globalization is not supported. Running in culture '
            'invariant mode (DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=\'true\').')
    else:
        log.error('Failed to check if globalization is supported.')
        log.debug('Error: ' + err)


def _format_with_session_id(message):
    return message + ' |session_id={}'.format(session_id)
