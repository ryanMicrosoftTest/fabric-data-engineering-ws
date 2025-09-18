import os
import threading
from uuid import uuid4

from azureml.dataprep import read_preppy, Dataflow, DataPrepException
from azureml.dataprep.api._loggerfactory import _LoggerFactory
from copy import deepcopy


log = _LoggerFactory.get_logger('dprep.fuse._cached_dataflow')


class CachedDataflow:
    def __init__(self, dataflow, cache_dir, show_not_found_error=True):
        self._dataflow = dataflow
        self._cache_dir = os.path.join(cache_dir, '__dprep_preppy_{}__'.format(str(uuid4())))
        self._cache_lock = threading.Lock()
        self._preppy_dataflow = None
        self.cache_available = False

        def _cache_to_preppy():
            log.debug('Caching thread started.')
            try:
                meta = deepcopy(self._dataflow._meta)
                meta['activity'] = 'mount.cache_dataflow'
                cache_dataflow = Dataflow(self._dataflow._engine_api, self._dataflow._steps, meta)
                cache_dataflow.write_to_preppy(self._cache_dir).run_local()
                self.cache_available = True
                log.debug('Caching dataflow to preppy is done')
            except DataPrepException as dpe:
                if 'NotFound' in dpe.error_code and not show_not_found_error:
                    _LoggerFactory.trace(log, 'Not found error during caching dataflow, ignoring. {}'.format(dpe.compliant_message))
                else:
                    log.warning('DataPrepException encountered while caching dataflow to preppy due to: {}'.format(dpe))
            except Exception as e:
                log.warning('Error encountered while caching dataflow to preppy due to: {}'.format(repr(e)))

        self._cache_to_preppy_fn = _cache_to_preppy
        self._start_caching()

    def dataflow(self, wait_for_cache):
        """Get the dataflow to consume, preferring to use preppy cache.

        :param wait_for_cache: wait for current caching attempt to complete.
        :type wait_for_cache: bool
        """
        if wait_for_cache:
            try:
                log.debug('Waiting for cache done.')
                # self._caching_thread is only assigned with started thread. It is always safe to call join
                self._caching_thread.join()
            except Exception as e:
                log.warning('Error encountered while waiting for cache done due to: {}'.format(repr(e)))
        try:
            # lock to block concurrenct attempt to start cache and create preppy dataflow
            self._cache_lock.acquire()
            if not os.path.exists(os.path.join(self._cache_dir, '_SUCCESS')):
                if not self._caching_thread.is_alive():
                    self._start_caching()
                log.info('Caching is in progress, returning original dataflow.')
                return self._dataflow
            if self._preppy_dataflow is None:
                self._preppy_dataflow = read_preppy(self._cache_dir + "/*.preppy", include_path=True, verify_exists=True)
            return self._preppy_dataflow
        except Exception as e:
            log.warning('Error encountered while reading cached dataflow from preppy due to: {}'.format(repr(e)))
            self._preppy_dataflow = None
            # fallback to use raw dataflow without cache
            return self._dataflow
        finally:
            self._cache_lock.release()

    def _start_caching(self):
        try:
            log.debug('Starting caching in another thread.')
            caching_thread = threading.Thread(target=self._cache_to_preppy_fn)
            caching_thread.start()
            self._caching_thread = caching_thread
        except Exception as e:
            log.warning('Failed to start caching thread due to: {}'.format(repr(e)))

class BypassCacheDataflow: 
    def __init__(self, dataflow):
        self.cache_available = True
        self._dataflow = dataflow

    def dataflow(self, wait_for_cache):
        return self._dataflow