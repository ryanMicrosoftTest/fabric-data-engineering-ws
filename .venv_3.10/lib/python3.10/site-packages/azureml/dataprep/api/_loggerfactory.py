import inspect
import logging
import logging.handlers
import tempfile
import uuid
import os
import json
import pkg_resources
from contextlib import contextmanager
from functools import wraps

from ._constants import ACTIVITY_INFO_KEY, ERROR_CODE_KEY, COMPLIANT_MESSAGE_KEY, OUTER_ERROR_CODE_KEY
from .tracing._application_insights_trace_exporter import OCApplicationInsightsTraceExporter
from .tracing._exporter import JsonLineExporter
from .tracing._run_history_exporter import RunHistoryExporter
from .tracing._span_processor import ExporterSpanProcessor, UserFacingSpanProcessor, DevFacingSpanProcessor,\
    VerbositySampledSpanProcessor, AmlContextSpanProcessor, AggregatedSpanProcessor
from .tracing._tracer import AmlTracer, DefaultTraceProvider


COMPONENT_NAME = 'azureml.dataprep'
session_id = 'l_' + str(uuid.uuid4())
instrumentation_key = ''
try:
    version = pkg_resources.get_distribution("azureml-dataprep").version
except:
    version = ''
default_custom_dimensions = {}
node_rank = os.environ.get('AZUREML_CR_NODE_RANK') 
run_id = os.environ.get('AZUREML_RUN_ID')
log_directory = os.path.join(tempfile.gettempdir(), 'azureml-logs', 'dataprep')
if run_id and node_rank:
    # override when in run context and multinode in CR
    log_directory = os.path.join(os.getcwd(), 'logs', 'azureml', 'dataprep', node_rank)
elif run_id:
    # override when in run context and not multinode 
    log_directory = os.path.join(os.getcwd(), 'logs', 'azureml', 'dataprep')

umask = os.umask(0)
try:
    os.makedirs(log_directory, exist_ok=True)
except FileExistsError:
    # there are instances where makedirs still fails for existing directory even with exists_ok=True
    pass
finally:
    os.umask(umask)
processors = []

STACK_FMT = "%s, line %d in function %s."

try:
    from azureml.telemetry import (get_telemetry_log_handler, INSTRUMENTATION_KEY, get_diagnostics_collection_info, set_diagnostics_collection as telemetry_set_diagnostics_collection,
                                   HBI_MODE)
    from azureml.telemetry.activity import log_activity as _log_activity, ActivityType, ActivityLoggerAdapter
    from azureml.telemetry.logging_handler import AppInsightsLoggingHandler
    from azureml.telemetry.loggers import Loggers
    from azureml._base_sdk_common import _ClientSessionId
    session_id = _ClientSessionId
    current_folder = os.path.dirname(os.path.realpath(__file__))
    telemetry_config_path = os.path.join(current_folder, '_telemetry.json')

    telemetry_enabled, verbosity = get_diagnostics_collection_info(component_name=COMPONENT_NAME, path=telemetry_config_path, proceed_in_hbi_mode=True)
    instrumentation_key = INSTRUMENTATION_KEY if telemetry_enabled else ''
    DEFAULT_ACTIVITY_TYPE = ActivityType.INTERNALCALL

    tracing_instrumentation_key = os.environ.get('AZUREML_DPREP_TRACING_APPINSIGHTS_KEY')
    if tracing_instrumentation_key:
        telemetry_client = Loggers.default_logger(instrumentation_key=tracing_instrumentation_key).telemetry_client
        processors.append(VerbositySampledSpanProcessor(DevFacingSpanProcessor(
            ExporterSpanProcessor(OCApplicationInsightsTraceExporter(telemetry_client))
        ), level=logging.NOTSET))
except Exception:
    HBI_MODE = False
    ActivityLoggerAdapter = None
    telemetry_enabled = False
    DEFAULT_ACTIVITY_TYPE = "InternalCall"
    verbosity = None

if os.environ.get('ENABLE_SPAN_JSONL'):
    processors.append(ExporterSpanProcessor(JsonLineExporter(session_id=session_id, base_directory=log_directory)))

if os.environ.get('AZUREML_OTEL_EXPORT_RH'):
    processors.append(UserFacingSpanProcessor(ExporterSpanProcessor(RunHistoryExporter())))

if os.environ.get('DPREP_ENABLE_JAEGER'):
    # https://msdata.visualstudio.com/Vienna/_workitems/edit/834099
    # processors.append(ExporterSpanProcessor(OCJaegerExporterAdapter()))
    pass

_propagate = False
if os.environ.get('DPREP_SDK_LOG_STDOUT'):
    _propagate = True

processor = AmlContextSpanProcessor(AggregatedSpanProcessor(processors))
_tracer = AmlTracer([processor])
trace = DefaultTraceProvider(_tracer)


class _LoggerFactory:
    @staticmethod
    def get_logger(name, verbosity=logging.DEBUG):
        logger = logging.getLogger(__name__).getChild(name)
        logger.propagate = _propagate
        logger.setLevel(verbosity)
        if telemetry_enabled:
            if not _LoggerFactory._found_handler(logger, AppInsightsLoggingHandler):
                logger.addHandler(get_telemetry_log_handler(component_name=COMPONENT_NAME, path=telemetry_config_path))

        return logger

    @staticmethod
    def track_activity(logger, activity_name, activity_type=DEFAULT_ACTIVITY_TYPE, custom_dimensions=None):
        stack = _LoggerFactory.get_stack()
        global default_custom_dimensions
        if custom_dimensions is not None:
            custom_dimensions = {**default_custom_dimensions, **custom_dimensions}
        else:
            custom_dimensions = default_custom_dimensions
        custom_dimensions.update({'source': COMPONENT_NAME, 'version': version, 'trace': stack})
        run_info = _LoggerFactory._try_get_run_info()
        if run_info is not None:
            # change to only update for key that doesnt exist
            for key, value in run_info.items():
                custom_dimensions.setdefault(key, value)
        if telemetry_enabled:
            return _log_activity(logger, activity_name, activity_type, custom_dimensions)
        else:
            return _run_without_logging(logger, activity_name, activity_type, custom_dimensions)

    @staticmethod
    def trace(logger, message, custom_dimensions=None):
        payload = dict(pid=os.getpid())
        global default_custom_dimensions
        if custom_dimensions is not None:
            custom_dimensions = {**default_custom_dimensions, **custom_dimensions}
        else:
            custom_dimensions = default_custom_dimensions
        payload.update(custom_dimensions)
        payload['version'] = version

        if ActivityLoggerAdapter:
            activity_logger = ActivityLoggerAdapter(logger, payload)
            activity_logger.info(message)
        else:
            logger.info('Message: {}\nPayload: {}'.format(message, json.dumps(payload)))

    @staticmethod
    def trace_warn(logger, message, custom_dimensions=None):
        payload = dict(pid=os.getpid())
        global default_custom_dimensions
        if custom_dimensions is not None:
            custom_dimensions = {**default_custom_dimensions, **custom_dimensions}
        else:
            custom_dimensions = default_custom_dimensions
        payload.update(custom_dimensions)
        payload['version'] = version

        if ActivityLoggerAdapter:
            activity_logger = ActivityLoggerAdapter(logger, payload)
            activity_logger.warning(message)
        else:
            logger.warning('Message: {}\nPayload: {}'.format(message, json.dumps(payload)))
    
    @staticmethod
    def trace_error(logger, message, custom_dimensions=None):
        payload = dict(pid=os.getpid())
        global default_custom_dimensions
        if custom_dimensions is not None:
            custom_dimensions = {**default_custom_dimensions, **custom_dimensions}
        else:
            custom_dimensions = default_custom_dimensions
        payload.update(custom_dimensions)
        payload['version'] = version

        if ActivityLoggerAdapter:
            activity_logger = ActivityLoggerAdapter(logger, payload)
            activity_logger.error(message)
        else:
            logger.error('Message: {}\nPayload: {}'.format(message, json.dumps(payload)))

    @staticmethod
    def trace_debug(logger, message, custom_dimensions=None):
        payload = dict(pid=os.getpid())
        global default_custom_dimensions
        if custom_dimensions is not None:
            custom_dimensions = {**default_custom_dimensions, **custom_dimensions}
        else:
            custom_dimensions = default_custom_dimensions
        payload.update(custom_dimensions)
        payload['version'] = version

        if ActivityLoggerAdapter:
            activity_logger = ActivityLoggerAdapter(logger, payload)
            activity_logger.debug(message)
        else:
            logger.debug('Message: {}\nPayload: {}'.format(message, json.dumps(payload)))


    @staticmethod
    def _found_handler(logger, handler_type):
        for log_handler in logger.handlers:
            if isinstance(log_handler, handler_type):
                return True

        return False

    @staticmethod
    def set_default_custom_dimensions(custom_dimensions):
        global default_custom_dimensions
        default_custom_dimensions = custom_dimensions

    @staticmethod
    def add_default_custom_dimensions(custom_dimensions):
        global default_custom_dimensions
        default_custom_dimensions = {**default_custom_dimensions, **custom_dimensions}

    @staticmethod
    def get_stack(limit=3, start=1) -> str:
        try:
            stack = inspect.stack()
            # The index of the first frame to print.
            begin = start + 2
            # The index of the last frame to print.
            if limit:
                end = min(begin + limit, len(stack))
            else:
                end = len(stack)

            lines = []
            for frame in stack[begin:end]:
                file, line, func = frame[1:4]
                parts = file.rsplit('\\', 4)
                parts = parts if len(parts) > 1 else file.rsplit('/', 4)
                file = '|'.join(parts[-3:])
                lines.append(STACK_FMT % (file, line, func))
            return '\n'.join(lines)
        except:
            pass
        return None

    @staticmethod
    def _try_get_run_info():
        try:
            import re
            location = os.environ.get("AZUREML_SERVICE_ENDPOINT")
            location = re.compile("//(.*?)\\.").search(location).group(1)
        except:
            location = os.environ.get("AZUREML_SERVICE_ENDPOINT", "")
        return {
            "subscription": os.environ.get('AZUREML_ARM_SUBSCRIPTION', ""),
            "run_id": os.environ.get("AZUREML_RUN_ID", ""),
            "resource_group": os.environ.get("AZUREML_ARM_RESOURCEGROUP", ""),
            "workspace_name": os.environ.get("AZUREML_ARM_WORKSPACE_NAME", ""),
            "experiment_id": os.environ.get("AZUREML_EXPERIMENT_ID", ""),
            "location": location
        }


def set_diagnostics_collection(send_diagnostics=True):
    try:
        telemetry_set_diagnostics_collection(send_diagnostics=send_diagnostics, path=telemetry_config_path)
        print("{} diagnostics collection. Changes will take effect in the next session.".format("Enabling" if send_diagnostics else "Disabling"))
    except: pass


def track(get_logger, custom_dimensions = None):
    def monitor(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger()
            with _LoggerFactory.track_activity(logger, func.__name__, DEFAULT_ACTIVITY_TYPE, custom_dimensions) as activityLogger:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if hasattr(activityLogger, ACTIVITY_INFO_KEY) and hasattr(e, ERROR_CODE_KEY):
                        activityLogger.activity_info['error_code'] = getattr(e, ERROR_CODE_KEY, '')
                        activityLogger.activity_info['message'] = getattr(e, COMPLIANT_MESSAGE_KEY, '')
                        activityLogger.activity_info['outer_error_code'] = getattr(e, OUTER_ERROR_CODE_KEY, '')
                    raise

        return wrapper

    return monitor

@contextmanager
def _run_without_logging(logger, activity_name, activity_type, custom_dimensions):
    yield logger
