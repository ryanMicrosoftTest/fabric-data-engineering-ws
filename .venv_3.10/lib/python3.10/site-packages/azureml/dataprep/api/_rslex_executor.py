import json
import os
import threading
from ._loggerfactory import _LoggerFactory, session_id, instrumentation_key, log_directory, verbosity, HBI_MODE
from typing import Optional, Dict

log = _LoggerFactory.get_logger('rslex_executor')
use_rslex = False if 'USE_CLEX' in os.environ and os.environ['USE_CLEX'] == 'True' else True

class _RsLexDisabledException(Exception):
    pass

class _RsLexExecutor:
    def execute_dataflow(self, script, collect_results, fail_on_error, fail_on_mixed_types, fail_on_out_of_range_datetime, traceparent, partition_ids=None):
        '''
        Takes a script to execute and execution properties. Attempts to execute the script in rslex and return record batches.
        '''
        if not use_rslex:
            raise _RsLexDisabledException()
            
        from azureml.dataprep.rslex import Executor
        ex = Executor()
        (batches, num_partitions, stream_columns) = ex.execute_dataflow(
            script,
            collect_results,
            partition_ids,
            fail_on_error,
            fail_on_mixed_types,
            fail_on_out_of_range_datetime,
            traceparent
        )
        log.info('Execution succeeded with {} partitions.'.format(num_partitions))
        return (batches, num_partitions, stream_columns)

    def get_partition_info(self, script, traceparent=''):
        from azureml.dataprep.rslex import Executor
        ex = Executor()
        (num_partitions, streams_with_partition_counts) = ex.get_partition_info(script, traceparent)
        log.info('Getting partition count succeeded with {} partitions. Streams were {} present.'.format(num_partitions, 'not' if streams_with_partition_counts is None else ''))
        return (num_partitions, streams_with_partition_counts)

    def get_partition_count(self, script, traceparent=''):
        '''
        Takes a script to execute and execution properties. Attempts to execute the script without collecting in rslex and return partition count.
        '''
        return self.execute_dataflow(
            script,
            collect_results=False,
            fail_on_error=False,
            fail_on_mixed_types=False,
            fail_on_out_of_range_datetime=False,
            traceparent=traceparent
        )[1]

_rslex_executor = None
_rslex_environment_init = False
_rslex_environment_lock = threading.Lock()


def get_rslex_executor():
    global _rslex_executor
    if _rslex_executor is None:
        _rslex_executor = _RsLexExecutor()
    ensure_rslex_environment()

    return _rslex_executor


def _set_rslex_environment(value: bool):
    global _rslex_environment_init
    _rslex_environment_lock.acquire()
    _rslex_environment_init = value
    _rslex_environment_lock.release()


def ensure_rslex_environment(caller_session_id: str = None, disk_space_overrides: Optional[Dict[str, int]] = None, metrics_endpoint: Optional[str] = None):
    global _rslex_environment_init
    if _rslex_environment_init is False:
        try:
            # Acquire lock on mutable access to _rslex_environment_init
            _rslex_environment_lock.acquire()
            # Was _rslex_environment_init set while we held the lock?
            if _rslex_environment_init is True:
                return _rslex_environment_init
            # Initialize new RsLex Environment
            import atexit
            import azureml.dataprep.rslex as rslex
            run_info = _LoggerFactory._try_get_run_info()
            rslex.init_environment(
                log_directory,
                None,  # by default rslex uses its own app insights key
                verbosity,
                HBI_MODE,
                session_id,
                caller_session_id,
                json.dumps(run_info) if run_info is not None else None,
                disk_space_overrides = disk_space_overrides,
                metrics_endpoint = metrics_endpoint
            )
            _rslex_environment_init = True
            _LoggerFactory.add_default_custom_dimensions({'rslex_version': rslex.__version__})
            atexit.register(rslex.release_environment)
        except Exception as e:
            log.error('ensure_rslex_environment failed with {}'.format(e))
            raise
        finally:
            if _rslex_environment_lock.locked():
                _rslex_environment_lock.release()
    return _rslex_environment_init


def use_rust_execution(use: bool):
    global use_rslex
    if use is True:
        ensure_rslex_environment()
    use_rslex = use
