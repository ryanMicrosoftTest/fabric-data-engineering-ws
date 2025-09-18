import re
from typing import List, Optional, Dict

from azureml.dataprep.api._loggerfactory import _LoggerFactory


_path_delimiter_re = re.compile(r'/|\\')


def get_trace_with_invocation_id(logger, invocation_id: str):
    def trace(message, custom_dimensions={}):
        custom_dimensions['invocation_id'] = invocation_id
        _LoggerFactory.trace(logger, message, custom_dimensions)

    return trace


def _log_process_info(logger, pid):
    status = _read_proc_kvp('/proc/{}/status'.format(pid), logger)
    try:
        # https://man7.org/linux/man-pages/man5/proc.5.html
        with open('/proc/{}/stat'.format(pid)) as f:
            status['stat'] = f.readline()
    except Exception as e:
        _LoggerFactory.trace(logger, 'Failed to get stat info for process {} due to {} with stacktrace {}.'.format(
            pid,
            type(e).__name__,
            _anonymize_stacktrace()
        ))

    _LoggerFactory.trace(logger, 'Process info for pid {}.'.format(pid), status)


def _log_resource_info(logger):
    status = _read_proc_kvp('/proc/meminfo', logger)
    _LoggerFactory.trace(logger, 'meminfo for node.', status)


def _read_proc_kvp(path: str, logger) -> Dict[str, str]:
    values = {}
    try:
        # https://man7.org/linux/man-pages/man5/proc.5.html
        with open(path) as f:
            for line in f.readlines():
                kvp = line.split(':')
                if len(kvp) < 2:
                    _LoggerFactory.trace(logger, 'Skipping entry {} as it cannot be splitted into key value pair.'.format(
                        line
                    ))
                    continue
                values[kvp[0]] = ':'.join(kvp[1:]).strip('\t ')
        return values
    except Exception as e:
        _LoggerFactory.trace(logger, 'Failed to read {} due to {} with stacktrace {}.'.format(
            path,
            type(e).__name__,
            _anonymize_stacktrace()
        ))
        return values


def _anonymize_stacktrace() -> Optional[List[str]]:
    import sys
    import traceback

    try:
        exc_type, exc_value, exc_tb = sys.exc_info()
        frames = traceback.extract_tb(exc_tb)
        return ['File {}, {} on {}, in {}'.format(
            _path_delimiter_re.split(frame.filename)[-1],
            frame.line,
            frame.lineno,
            frame.name
        ) for frame in frames]
    except:
        return []


def _set_default_custom_dimensions():
    _LoggerFactory.add_default_custom_dimensions(_LoggerFactory._try_get_run_info())
