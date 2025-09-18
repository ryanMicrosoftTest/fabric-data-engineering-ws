# Copyright (c) Microsoft Corporation. All rights reserved.
# pylint: skip-file
# This file is auto-generated. Do not modify.
from typing import List, Dict
from uuid import UUID
from .enginerequests import EngineRequestsChannel
from .engine import launch_engine, CancellationToken
from . import typedefinitions
from .._aml_helper import update_aml_env_vars
from .._loggerfactory import _LoggerFactory, trace


_engine_api = None


def get_engine_api():
    global _engine_api
    if not _engine_api:
        _engine_api = EngineAPI()

        from .._dataset_resolver import register_dataset_resolver
        register_dataset_resolver(_engine_api.requests_channel)

        from .._aml_auth_resolver import register_datastore_resolver
        register_datastore_resolver(_engine_api.requests_channel)

        from .._azure_access_token_resolver import register_access_token_resolver
        register_access_token_resolver(_engine_api.requests_channel)

        from ..sparkexecution import _ensure_interactive_spark
        _ensure_interactive_spark(_engine_api.requests_channel)

        from .._dataframereader import ensure_dataframe_reader_handlers
        ensure_dataframe_reader_handlers(_engine_api.requests_channel)

        from .._partitionsreader import ensure_partitions_reader_handlers
        ensure_partitions_reader_handlers(_engine_api.requests_channel)

    return _engine_api


def kill_engine_api():
    global _engine_api
    if _engine_api:
        _engine_api._message_channel.close()
        _engine_api = None

def _try_rslex_upload(destination, source_path, base_dir, overwrite, is_volume=False):
    logger = _LoggerFactory.get_logger('dprep.rslex_upload')
    tracer = trace.get_tracer(__name__)

    try:
        from azureml.dataprep.rslex import Copier, PyLocationInfo, StreamInfo, PyIfDestinationExists
        from azureml.dataprep.api._rslex_executor import ensure_rslex_environment
        import json, os
        if os.environ.get('DATASET_RSLEX_UPLOAD', 'true').lower() != 'true':
            return False

        with tracer.start_as_current_span('_try_rslex_upload', trace.get_current_span()):
            logger.info('Trying rslex upload')

            logger.info('Ensure rslex environment')
            ensure_rslex_environment()

            logger.info('Creating rslex copier')
            stream_info = destination.to_pod()['streaminfo']
            arguments = stream_info['arguments']
            destination_info = PyLocationInfo(
                stream_info['handler'],
                stream_info['resourceidentifier'],
                {
                    'subscription': arguments['subscription']['string'],
                    'resourceGroup': arguments['resourceGroup']['string'],
                    'workspaceName': arguments['workspaceName']['string'],
                    'datastoreName': arguments['datastoreName']['string']
                })
            copier = Copier(
                destination_info,
                base_dir,
                PyIfDestinationExists.MERGE_WITH_OVERWRITE if overwrite else PyIfDestinationExists.APPEND)

            logger.info('Starting upload')
            if is_volume:
                volume_info = PyLocationInfo('Local', source_path, { })
                copier.copy_volume(volume_info, trace.get_current_span().to_w3c_traceparent())
            else:
                source_stream_info = StreamInfo('Local', source_path, {})
                copier.copy_stream_info(source_stream_info, trace.get_current_span().to_w3c_traceparent())
            return True
    except Exception as e:
        logger.error('Uploading with rslex failed with error {}. Falling back to clex'.format(e))
        return False

class EngineAPI:
    def __init__(self):
        self.requests_channel = EngineRequestsChannel()

        def connect_to_requests_channel():
            self._engine_server_secret = self.sync_host_secret(self.requests_channel.host_secret)
            self._engine_server_port = self.sync_host_channel_port(self.requests_channel.port)

        self._message_channel = launch_engine()
        connect_to_requests_channel()

        self._message_channel.on_relaunch(connect_to_requests_channel)

    @update_aml_env_vars(get_engine_api)
    def add_block_to_list(self, message_args: typedefinitions.AddBlockToListMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousBlockData:
        response = self._message_channel.send_message('Engine.AddBlockToList', message_args, cancellation_token)
        return typedefinitions.AnonymousBlockData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def add_temporary_secrets(self, message_args: Dict[str, str], cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.AddTemporarySecrets', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def anonymous_data_source_prose_suggestions(self, message_args: typedefinitions.AnonymousDataSourceProseSuggestionsMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.DataSourceProperties:
        response = self._message_channel.send_message('GetProseAnonymousDataSourcePropertiesSuggestion', message_args, cancellation_token)
        return typedefinitions.DataSourceProperties.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def anonymous_send_message_to_block(self, message_args: typedefinitions.AnonymousSendMessageToBlockMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousSendMessageToBlockMessageResponseData:
        response = self._message_channel.send_message('Engine.SendMessageToBlock', message_args, cancellation_token)
        return typedefinitions.AnonymousSendMessageToBlockMessageResponseData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def close_stream_info(self, message_args: str, cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.CloseStreamInfo', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def convert_rsflow_to_dataflow_json(self, message_args: typedefinitions.ConvertRsflowToDataflowJsonMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousActivityData:
        response = self._message_channel.send_message('Engine.ConvertRsflowToDataflow', message_args, cancellation_token)
        return typedefinitions.AnonymousActivityData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def create_anonymous_reference(self, message_args: typedefinitions.CreateAnonymousReferenceMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.ActivityReference:
        response = self._message_channel.send_message('Engine.CreateAnonymousReference', message_args, cancellation_token)
        return typedefinitions.ActivityReference.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def create_folder(self, message_args: typedefinitions.CreateFolderMessageArguments, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.CreateFolder', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def delete(self, message_args: typedefinitions.DeleteMessageArguments, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.Delete', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def download_stream_info(self, message_args: typedefinitions.DownloadStreamInfoMessageArguments, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.DownloadStreamInfo', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def execute_anonymous_activity(self, message_args: typedefinitions.ExecuteAnonymousActivityMessageArguments, cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.ExecuteActivity', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def execute_data_diff(self, message_args: typedefinitions.ExecuteDataDiffMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.ExecuteDataDiffMessageResponse:
        response = self._message_channel.send_message('Engine.ExecuteDataDiff', message_args, cancellation_token)
        return typedefinitions.ExecuteDataDiffMessageResponse.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def execute_inspector(self, message_args: typedefinitions.ExecuteInspectorCommonArguments, cancellation_token: CancellationToken = None) -> typedefinitions.ExecuteInspectorCommonResponse:
        response = self._message_channel.send_message('Engine.ExecuteInspector', message_args, cancellation_token)
        return typedefinitions.ExecuteInspectorCommonResponse.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def execute_inspectors(self, message_args: List[typedefinitions.ExecuteInspectorsMessageArguments], cancellation_token: CancellationToken = None) -> Dict[str, typedefinitions.ExecuteInspectorCommonResponse]:
        response = self._message_channel.send_message('Engine.ExecuteInspectors', message_args, cancellation_token)
        return {k: typedefinitions.ExecuteInspectorCommonResponse.from_pod(v) if v is not None else None for k, v in response.items()} if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def export_script(self, message_args: typedefinitions.ExportScriptMessageArguments, cancellation_token: CancellationToken = None) -> List[typedefinitions.SecretData]:
        response = self._message_channel.send_message('Project.ExportScript', message_args, cancellation_token)
        return [typedefinitions.SecretData.from_pod(i) if i is not None else None for i in response] if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def file_exists(self, message_args: typedefinitions.FileExistsMessageArguments, cancellation_token: CancellationToken = None) -> bool:
        response = self._message_channel.send_message('Engine.FileExists', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def get_activity(self, message_args: str, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousActivityData:
        response = self._message_channel.send_message('Engine.GetActivity', message_args, cancellation_token)
        return typedefinitions.AnonymousActivityData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_block_descriptions(self, message_args: None, cancellation_token: CancellationToken = None) -> List[typedefinitions.IBlockDescription]:
        response = self._message_channel.send_message('Engine.GetBlockDescriptions', message_args, cancellation_token)
        return [typedefinitions.IBlockDescription.from_pod(i) if i is not None else None for i in response] if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_data(self, message_args: typedefinitions.GetDataMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.GetDataMessageResponse:
        response = self._message_channel.send_message('Engine.GetData', message_args, cancellation_token)
        return typedefinitions.GetDataMessageResponse.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_inspector_descriptions(self, message_args: None, cancellation_token: CancellationToken = None) -> List[typedefinitions.InspectorDescription]:
        response = self._message_channel.send_message('Engine.GetInspectorDescriptions', message_args, cancellation_token)
        return [typedefinitions.InspectorDescription.from_pod(i) if i is not None else None for i in response] if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_inspector_lariat(self, message_args: typedefinitions.ExecuteInspectorCommonArguments, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Engine.GetInspectorLariat', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def get_partition_count(self, message_args: List[typedefinitions.AnonymousBlockData], cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.GetPartitionCount', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def get_program_step_descriptions(self, message_args: None, cancellation_token: CancellationToken = None) -> List[typedefinitions.ProgramStepDescription]:
        response = self._message_channel.send_message('Engine.GetProgramStepDescriptions', message_args, cancellation_token)
        return [typedefinitions.ProgramStepDescription.from_pod(i) if i is not None else None for i in response] if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_script(self, message_args: typedefinitions.GetScriptMessageArguments, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Engine.GetRsLexScript', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def get_secrets(self, message_args: typedefinitions.GetSecretsMessageArguments, cancellation_token: CancellationToken = None) -> List[typedefinitions.SecretData]:
        response = self._message_channel.send_message('Engine.GetSecrets', message_args, cancellation_token)
        return [typedefinitions.SecretData.from_pod(i) if i is not None else None for i in response] if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def get_source_data_hash(self, message_args: typedefinitions.GetSourceDataHashMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.GetSourceDataHashMessageResponse:
        response = self._message_channel.send_message('Engine.GetSourceDataHash', message_args, cancellation_token)
        return typedefinitions.GetSourceDataHashMessageResponse.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def infer_types(self, message_args: List[typedefinitions.AnonymousBlockData], cancellation_token: CancellationToken = None) -> Dict[str, typedefinitions.FieldInference]:
        response = self._message_channel.send_message('Engine.InferTypes', message_args, cancellation_token)
        return {k: typedefinitions.FieldInference.from_pod(v) if v is not None else None for k, v in response.items()} if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def infer_types_with_span_context(self, message_args: typedefinitions.InferTypesWithSpanContextMessageArguments, cancellation_token: CancellationToken = None) -> Dict[str, typedefinitions.FieldInference]:
        response = self._message_channel.send_message('Engine.InferTypesWithSpanContextMessage', message_args, cancellation_token)
        return {k: typedefinitions.FieldInference.from_pod(v) if v is not None else None for k, v in response.items()} if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def load_activity_from_json(self, message_args: str, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousActivityData:
        response = self._message_channel.send_message('Engine.LoadProjectFromJson', message_args, cancellation_token)
        return typedefinitions.AnonymousActivityData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def load_activity_from_package(self, message_args: str, cancellation_token: CancellationToken = None) -> typedefinitions.AnonymousActivityData:
        response = self._message_channel.send_message('Activity.LoadFromPackage', message_args, cancellation_token)
        return typedefinitions.AnonymousActivityData.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def move_file(self, message_args: typedefinitions.MoveFileMessageArguments, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.MoveFile', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def open_stream_info(self, message_args: typedefinitions.Value, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Engine.OpenStreamInfo', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def read_stream_info(self, message_args: typedefinitions.ReadStreamInfoMessageArguments, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.ReadStreamInfo', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def register_secret(self, message_args: typedefinitions.RegisterSecretMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.Secret:
        response = self._message_channel.send_message('SecretManager.RegisterSecret', message_args, cancellation_token)
        return typedefinitions.Secret.from_pod(response) if response is not None else None

    @update_aml_env_vars(get_engine_api)
    def save_activity_from_data(self, message_args: typedefinitions.SaveActivityFromDataMessageArguments, cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Project.SaveAnonymous', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def save_activity_to_json(self, message_args: typedefinitions.AnonymousActivityData, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Project.SaveAnonymousToJson', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def save_activity_to_package(self, message_args: typedefinitions.AnonymousActivityData, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Activity.SaveToPackage', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def set_aml_auth(self, message_args: typedefinitions.SetAmlAuthMessageArgument, cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.SetAmlAuth', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def set_executor(self, message_args: typedefinitions.ExecutorType, cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.SetExecutor', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def sync_host_channel_port(self, message_args: int, cancellation_token: CancellationToken = None) -> int:
        response = self._message_channel.send_message('Engine.SyncHostChannelPort', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def sync_host_secret(self, message_args: str, cancellation_token: CancellationToken = None) -> str:
        response = self._message_channel.send_message('Engine.SyncHostSecret', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def update_environment_variable(self, message_args: Dict[str, str], cancellation_token: CancellationToken = None) -> None:
        response = self._message_channel.send_message('Engine.UpdateEnvironmentVariable', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def upload_directory(self, message_args: typedefinitions.UploadDirectoryMessageArguments, cancellation_token: CancellationToken = None) -> None:
        if _try_rslex_upload(message_args.destination, message_args.folder_path, message_args.base_path, message_args.overwrite, True):
            return None
        response = self._message_channel.send_message('Engine.UploadDirectory', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def upload_file(self, message_args: typedefinitions.UploadFileMessageArguments, cancellation_token: CancellationToken = None) -> None:
        if _try_rslex_upload(message_args.destination, message_args.local_path, message_args.base_path, message_args.overwrite):
            return None
        response = self._message_channel.send_message('Engine.UploadFile', message_args, cancellation_token)
        return response

    @update_aml_env_vars(get_engine_api)
    def validate_activity_source(self, message_args: typedefinitions.ValidateActivitySourceMessageArguments, cancellation_token: CancellationToken = None) -> typedefinitions.ValidationResult:
        response = self._message_channel.send_message('Engine.ValidateActivitySource', message_args, cancellation_token)
        return typedefinitions.ValidationResult.from_pod(response) if response is not None else None
