# Copyright (c) Microsoft Corporation. All rights reserved.
from .engineapi.typedefinitions import AuthType
from ... import dataprep
from typing import TypeVar, List
import json

DEFAULT_SAS_DURATION = 30  # this aligns with our SAS generation in the UI BlobStorageManager.ts
Datastore = TypeVar('Datastore', 'AbstractDatastore', 'DataReference', 'DataPath')
Datastores = TypeVar('Datastores', Datastore, List[Datastore])


def datastore_to_dataflow(data_source: Datastores, query_timeout: int=-1, is_file: bool=False) -> 'dataprep.Dataflow':
    from .dataflow import Dataflow, DatastoreValue
    from .engineapi.api import get_engine_api

    datastore_key = 'datastore'
    datastore_value_key = 'datastore_value'

    def construct_df(datastore_values, is_file):
        if type(datastore_values) is not list:
            datastore_values = [datastore_values]
        df = Dataflow(get_engine_api())
        return add_create_dataset_block_step(df, datastore_values) if is_file \
            else add_get_datastore_files_block_step(df, datastore_values)

    def add_get_datastore_files_block_step(df, datastore_values):
        return df.add_step('Microsoft.DPrep.GetDatastoreFilesBlock', {
            'datastores': [datastore_value._to_pod() for datastore_value in datastore_values]
        })

    def add_create_dataset_block_step(df, datastore_values):
        return df.add_step('Microsoft.DPrep.CreateDatasetBlock', {
            'streamInfos': [to_stream_info_pod(datastore_value) for datastore_value in datastore_values]
        })

    if type(data_source) is list:
        datastore_values = []
        for source in data_source:
            datastore, datastore_value = get_datastore_value(source)
            if not _is_fs_datastore(datastore):
                raise NotSupportedDatastoreTypeError(datastore)
            datastore_values.append(datastore_value)
        return construct_df(datastore_values, is_file)

    datastore, datastore_value = get_datastore_value(data_source)
    if _is_fs_datastore(datastore):
        return construct_df(datastore_value, is_file)

    try:
        from azureml.data.azure_sql_database_datastore import AzureSqlDatabaseDatastore
        from azureml.data.azure_postgre_sql_datastore import AzurePostgreSqlDatastore
    except ImportError as e:
        raise _decorate_import_error(e)

    if isinstance(datastore, AzureSqlDatabaseDatastore) or isinstance(datastore, AzurePostgreSqlDatastore):
        if query_timeout == -1:
            query_timeout = 30 if isinstance(datastore, AzureSqlDatabaseDatastore) else 20
        df = Dataflow(get_engine_api())
        return df.add_step('Microsoft.DPrep.ReadDatastoreSqlBlock', {
                               'datastore': datastore_value._to_pod(),
                               'queryTimeout': query_timeout
                           })

    raise NotSupportedDatastoreTypeError(datastore)


def get_datastore_value(data_source: Datastore) -> ('AbstractDatastore', 'dataprep.api.dataflow.DatastoreValue'):
    from .dataflow import DatastoreValue
    try:
        from azureml.data.abstract_datastore import AbstractDatastore
        from azureml.data.data_reference import DataReference
        from azureml.data.datapath import DataPath
    except ImportError as e:
        raise _decorate_import_error(e)

    datastore = None
    path_on_storage = ''

    if isinstance(data_source, AbstractDatastore):
        datastore = data_source
    elif isinstance(data_source, DataReference):
        datastore = data_source.datastore
        path_on_storage = data_source.path_on_datastore or path_on_storage
    elif isinstance(data_source, DataPath):
        datastore = data_source._datastore
        path_on_storage = data_source.path_on_datastore or path_on_storage

    _ensure_supported(datastore)

    workspace = datastore.workspace
    _set_auth_type(workspace)
    return (datastore, DatastoreValue(
        subscription=workspace.subscription_id,
        resource_group=workspace.resource_group,
        workspace_name=workspace.name,
        datastore_name=datastore.name,
        path=path_on_storage
    ))


def to_stream_info_pod(datastore_value: 'dataprep.api.dataflow.DatastoreValue') -> dict:
    return {
        'handler': 'AmlDatastore',
        'resourceidentifier': datastore_value.datastore_name + '/' + datastore_value.path.lstrip('/'),
        'arguments': {
            'subscription': datastore_value.subscription,
            'resourceGroup': datastore_value.resource_group,
            'workspaceName': datastore_value.workspace_name,
            'datastoreName': datastore_value.datastore_name
        }
    }


def login():
    try:
        from azureml.core.authentication import InteractiveLoginAuthentication
    except ImportError as e:
        raise _decorate_import_error(e)

    auth = InteractiveLoginAuthentication()
    auth.get_authentication_header()


def _ensure_supported(datastore: 'AbstractDatastore'):
    try:
        from azureml.data.azure_sql_database_datastore import AzureSqlDatabaseDatastore
        from azureml.data.azure_postgre_sql_datastore import AzurePostgreSqlDatastore
    except ImportError as e:
        raise _decorate_import_error(e)

    if _is_fs_datastore(datastore) or isinstance(datastore, AzureSqlDatabaseDatastore) \
            or isinstance(datastore, AzurePostgreSqlDatastore):
        return
    raise NotSupportedDatastoreTypeError(datastore)


auth_type = AuthType.DERIVED
auth_value = {}


def _get_auth():
    global auth_type
    global auth_value
    return auth_type, auth_value


def _set_auth_type(workspace: 'Workspace'):
    global auth_type
    global auth_value

    from .engineapi.api import get_engine_api
    from .engineapi.typedefinitions import SetAmlAuthMessageArgument
    from ._aml_auth_resolver import WorkspaceContextCache
    try:
        from azureml.core.authentication import ServicePrincipalAuthentication
    except ImportError as e:
        raise _decorate_import_error(e)

    WorkspaceContextCache.add(workspace)

    auth = {}
    _try_update_auth(lambda: workspace._auth._cloud_type.name, auth, 'cloudType')
    _try_update_auth(lambda: workspace._auth._tenant_id, auth, 'tenantId')
    _try_update_auth(lambda: type(workspace._auth).__name__, auth, 'authClass')
    _try_update_auth(lambda: workspace._auth._cloud_type.endpoints.active_directory, auth, 'authority')
    if isinstance(workspace._auth, ServicePrincipalAuthentication):
        auth = {
            **auth,
            'servicePrincipalId': workspace._auth._service_principal_id,
            'password': workspace._auth._service_principal_password
        }
        auth_type = AuthType.SERVICEPRINCIPAL
    else:
        auth_type = AuthType.DERIVED

    auth_value = auth
    get_engine_api().set_aml_auth(SetAmlAuthMessageArgument(auth_type, json.dumps(auth_value)))

def _set_clould_type(cloud: "Cloud"):
    from .engineapi.api import get_engine_api
    from .engineapi.typedefinitions import SetAmlAuthMessageArgument

    global auth_type
    global auth_value

    auth = auth_value.copy()
    _try_update_auth(lambda: cloud.name, auth, 'cloudType')
    _try_update_auth(lambda: cloud.endpoints.active_directory, auth, 'authority')
    auth_value = auth
    get_engine_api().set_aml_auth(SetAmlAuthMessageArgument(auth_type, json.dumps(auth_value)))

def _try_update_auth(attr_getter, auth_dict, key):
    try:
        auth_dict[key] = attr_getter()
    except AttributeError:
        # this happens when the azureml-core SDK is old (prior to sovereign cloud changes)
        return


def _all(items, predicate):
    return len(list(filter(lambda ds: not predicate(ds), items))) == 0


def _is_datapath(data_path) -> bool:
    try:
        from azureml.data.data_reference import DataReference
        from azureml.data.abstract_datastore import AbstractDatastore
        from azureml.data.datapath import DataPath
    except ImportError as e:
        raise _decorate_import_error(e)

    return isinstance(data_path, DataReference) or \
            isinstance(data_path, AbstractDatastore) or \
            isinstance(data_path, DataPath)


def _is_datapaths(data_paths) -> bool:
    return type(data_paths) is list and _all(data_paths, _is_datapath)


def _is_fs_datastore(datastore: 'AbstractDatastore') -> bool:
    try:
        from azureml.data.azure_storage_datastore import AzureBlobDatastore
        from azureml.data.azure_storage_datastore import AzureFileDatastore
        from azureml.data.azure_data_lake_datastore import AzureDataLakeDatastore
        from azureml.data.azure_data_lake_datastore import AzureDataLakeGen2Datastore
    except ImportError as e:
        raise _decorate_import_error(e)
    
    is_fs_datastore = False
    try:
        from azureml.data.hdfs_datastore import HDFSDatastore
        is_fs_datastore = is_fs_datastore or isinstance(datastore, HDFSDatastore)
    except ImportError as e:
        if datastore.datastore_type == "Hdfs":
            raise _decorate_import_error(e)

    return isinstance(datastore, AzureBlobDatastore) or \
            isinstance(datastore, AzureFileDatastore) or \
            isinstance(datastore, AzureDataLakeDatastore) or \
            isinstance(datastore, AzureDataLakeGen2Datastore) or \
            datastore.datastore_type == "Custom" or \
            is_fs_datastore


def _to_stream_info_value(datastore: 'AbstractDatastore', path_in_datastore: str = '') -> dict:
    def to_value(value):
        return {'string': value}

    return {
        'streaminfo': {
            'handler': 'AmlDatastore',
            'resourceidentifier': datastore.name + '/' + path_in_datastore.lstrip('/'),
            'arguments': {
                'subscription': to_value(datastore.workspace.subscription_id),
                'resourceGroup': to_value(datastore.workspace.resource_group),
                'workspaceName': to_value(datastore.workspace.name),
                'datastoreName': to_value(datastore.name)
            }
        }
    }


def _serialize_datastore(datastore):
    return {
        'workspace': datastore.workspace.name,
        'subscription_id': datastore.workspace.subscription_id,
        'resource_group': datastore.workspace.resource_group,
        'datastore': datastore.name
    }


def _deserialize_datastore(datastore_dict):
    try:
        from azureml.core import Workspace, Run, Datastore as DatastoreClient
    except ImportError as e:
        raise _decorate_import_error(e)

    try:
        workspace = Run.get_context().experiment.workspace
    except AttributeError:
        workspace = Workspace.get(
            name=datastore_dict['workspace'], subscription_id=datastore_dict['subscription_id'],
            resource_group=datastore_dict['resource_group']
        )
    return DatastoreClient(workspace, datastore_dict['datastore'])


def _decorate_import_error(e: ImportError) -> ImportError:
    return ImportError('Unable to import Azure Machine Learning SDK. In order to use datastore, please make '\
                       + 'sure the Azure Machine Learning SDK is installed.\n{}'.format(e))


class NotSupportedDatastoreTypeError(Exception):
    def __init__(self, datastore: 'AbstractDatastore'):
        super().__init__('Datastore "{}"\'s type "{}" is not supported.'.format(datastore.name, datastore.datastore_type))
        self.datastore = datastore
