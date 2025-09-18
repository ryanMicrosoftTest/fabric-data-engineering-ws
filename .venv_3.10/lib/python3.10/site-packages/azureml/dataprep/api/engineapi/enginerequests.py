import json
import socketserver
import threading
from uuid import uuid4
from .._loggerfactory import _LoggerFactory

log = _LoggerFactory.get_logger('dprep.enginerequests')

class EngineRequestsChannel:
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                with self.request.makefile(encoding='utf8') as reader:
                    with self.request.makefile(mode='w', encoding='utf8') as writer:
                        try:
                            request = json.loads(reader.readline())
                        except Exception as e:
                            log.warn('[Handler.handle()] Failed to read or parse request from socket: {}'.format(e))
                            raise e

                        request_secret = request.get('host_secret')
                        if request_secret is None or request_secret != self.server.host_secret:
                            writer.write(json.dumps({'result': 'error', 'error': 'Unauthorized'}))
                        else:
                            try:
                                operation = request['operation']
                                callback = self.server.handlers.get(operation)
                                if callback is None:
                                    writer.write(json.dumps({'result': 'error', 'error': 'InvalidOperation'}))
                                else:
                                    callback(request, writer, self.request)
                            except Exception as e:
                                log.error('[Handler.handle()] Failed to handle operation due to exception class={}, message={}'.format(type(e).__name__, e))
                                writer.write(json.dumps({'result': 'error', 'error': repr(e)}) + '\n')
            except Exception as e:
                log.error('[Handler.handle()] Failed to handle request due to exception class={}, message={}'.format(type(e).__name__, e))

    def __init__(self):
        self._handlers = {}
        self._server = socketserver.ThreadingTCPServer(("localhost", 0), EngineRequestsChannel.Handler, False)
        self._server.daemon_threads = True
        # This is the number of requests we can queue up before rejecting connections. Given the engine
        # will be making a request per concurrent partition, 256 gives us enough headroom to handle
        # machines with up to 256 cores.
        self._server.request_queue_size = 256
        self._server.server_bind()
        self._server.server_activate()
        self._server.handlers = self._handlers
        self._server.host_secret = str(uuid4())
        self._server_thread = threading.Thread(target=self._server.serve_forever)
        self._server_thread.daemon = True
        self._server_thread.start()

    @property
    def port(self):
        return self._server.server_address[1]

    @property
    def host_secret(self):
        return self._server.host_secret

    def register_handler(self, message: str, callback):
        self._handlers[message] = callback

    def has_handler(self, message: str) -> bool:
        return message in self._handlers
