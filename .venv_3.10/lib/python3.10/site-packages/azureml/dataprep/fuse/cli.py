import argparse
import json
import os
import pickle
import azureml.dataprep as dprep

from azureml.dataprep.api._loggerfactory import _LoggerFactory
from azureml.dataprep.fuse._init_event_logger import FileInitEventLogger, NoopInitEventLogger
from azureml.dataprep.fuse._logger_helper import _anonymize_stacktrace, _set_default_custom_dimensions


logger = _LoggerFactory.get_logger('dprep.fuse.cli')


def _main():
    _set_default_custom_dimensions()

    arg_parser = argparse.ArgumentParser(description='Azure Machine Learning DPrepFuse CLI.')

    arg_parser.add_argument('--mount-args-path', required=True, type=str,
                            help='The path to a pickle file containing the arguments to the mount function.')
    arg_parser.add_argument('--dataflow-path', type=str, help='The path to a file containing the dataflow json.',
                            default=None)
    arg_parser.add_argument('--init-events-path', required=True, type=str,
                            help='The path to write initialization events to.')

    args = arg_parser.parse_args()
    init_logger = NoopInitEventLogger()
    if args.init_events_path:
        try:
            init_logger = FileInitEventLogger(args.init_events_path)
        except Exception as e:
            _LoggerFactory.trace(logger, 'Failed to initialize FileInitEventLogger due to {} with stacktrace {}.'.format(
                type(e).__name__,
                json.dumps(_anonymize_stacktrace())
            ))

    _LoggerFactory.trace(logger, 'Mount daemon process started.')
    init_logger.log('Mount daemon process started.')

    try:
        from azureml.dataprep.fuse.dprepfuse import clex_mount
        from azureml.dataprep.api.engineapi.engine import use_multi_thread_channel

        use_multi_thread_channel()
        init_logger.log('Use multi-threaded channel.')

        dataflow = None
        if args.dataflow_path is not None and os.path.exists(args.dataflow_path):
            init_logger.log('Loading dataflow.')
            with open(args.dataflow_path, 'r') as f:
                dataflow_json = f.read()
                if dataflow_json:
                    dataflow = dprep.Dataflow.from_json(dataflow_json)
                    init_logger.log('Loaded dataflow')

        init_logger.log('Loading mount arguments.')
        with open(args.mount_args_path, 'rb') as f:
            kwargs = pickle.load(f)
            init_logger.log('Loaded mount arguments.')

        caller_session_id = kwargs.get('caller_session_id')
        if caller_session_id is not None:
            _LoggerFactory.add_default_custom_dimensions({'caller_session_id': caller_session_id})

        if kwargs['dest']:
            init_logger.log('Updating destination.')
            kwargs['destination'] = (
                dprep.api._datastore_helper._deserialize_datastore(kwargs['dest'][0]),
                kwargs['dest'][1]
            )
            init_logger.log('Updated destination.')

        init_logger.log('Initializing mount.')
        clex_mount(dataflow, **kwargs)
    except Exception as e:
        error_message = 'Failed to start mount due to an exception of type {} with stacktrace {}'.format(
            type(e).__name__,
            json.dumps(_anonymize_stacktrace())
        )
        init_logger.log(error_message)
        logger.error(error_message)
        raise
    finally:
        init_logger.end()


if __name__ == '__main__':
    _main()
