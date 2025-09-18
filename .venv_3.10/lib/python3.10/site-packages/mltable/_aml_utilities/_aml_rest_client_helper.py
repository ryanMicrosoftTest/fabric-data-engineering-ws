# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

"""Contains helper methods for asset service REST APIs."""

import re
import os
from azureml.dataprep.api._loggerfactory import _LoggerFactory
from ._restclient._azure_machine_learning_workspaces import AzureMachineLearningWorkspaces as rest_client
from ._azureml_token_authentication import AzureMLTokenAuthentication

# ENV VARIABLE KEY FROM RUN CONTEXT
HISTORY_SERVICE_ENDPOINT_KEY = 'AZUREML_SERVICE_ENDPOINT'
AZUREML_ARM_SUBSCRIPTION_KEY = 'AZUREML_ARM_SUBSCRIPTION'
AZUREML_ARM_RESOURCEGROUP_KEY = 'AZUREML_ARM_RESOURCEGROUP'
AZUREML_ARM_WORKSPACE_NAME_KEY = 'AZUREML_ARM_WORKSPACE_NAME'

# SOTRAGE OPTIONS KEY
STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION = 'subscription'
STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP = 'resource_group'
STORAGE_OPTION_KEY_AZUREML_WORKSPACE = 'workspace'
STORAGE_OPTION_KEY_AZUREML_LOCATION = 'location'
STORAGE_OPTION_KEY_AZUREML_CREDENTIAL = 'credential'

_logger = None


def _get_logger():
    global _logger
    if _logger is None:
        _logger = _LoggerFactory.get_logger(__name__)
    return _logger


def _try_resolve_workspace_info(storage_options=None):
    # try to get workspace information from environment variable for remote run
    try:
        subscription_id = os.environ.get(AZUREML_ARM_SUBSCRIPTION_KEY)
        resource_group = os.environ.get(AZUREML_ARM_RESOURCEGROUP_KEY)
        workspace_name = os.environ.get(AZUREML_ARM_WORKSPACE_NAME_KEY)

        if storage_options is None:
            storage_options = {}

        if subscription_id is not None:
            storage_options[STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION] = subscription_id

        if resource_group is not None:
            storage_options[STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP] = resource_group

        if workspace_name is not None:
            storage_options[STORAGE_OPTION_KEY_AZUREML_WORKSPACE] = workspace_name

        return storage_options
    except:
        pass


def _has_sufficient_workspace_info(workspace_info):
    return workspace_info \
            and workspace_info.get(STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION, None) \
            and workspace_info.get(STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP, None) \
            and workspace_info.get(STORAGE_OPTION_KEY_AZUREML_WORKSPACE, None)


def _try_get_and_validate_workspace_info(storage_options=None):
    # try to get workspace information from environment variable for remote run
    workspace_info = _try_resolve_workspace_info(storage_options)
    if not _has_sufficient_workspace_info(workspace_info):
        _LoggerFactory.trace(_logger, 'storage_options is missing for fetching data asset')
        raise ValueError(f'Missing required workspace information '
                         f'`{STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION}`, '
                         f'`{STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP}`, '
                         f'`{STORAGE_OPTION_KEY_AZUREML_WORKSPACE}`')

    return workspace_info


def _get_aml_service_base_url(location=None):
    host_env = os.environ.get(HISTORY_SERVICE_ENDPOINT_KEY)

    # default to master
    if host_env is None:
        if location is None or location == 'centraluseuap':
            host_env = 'https://int.api.azureml-test.ms'
        else:
            host_env = F'https://{location}.api.azureml.ms'

    return host_env


def _get_rest_client(storage_options, auth):
    location = storage_options.get(STORAGE_OPTION_KEY_AZUREML_LOCATION, None)
    base_url = _get_aml_service_base_url(location)
    return rest_client(
        credential=auth,
        subscription_id=storage_options[STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION],
        base_url=base_url)


def _get_rest_client_in_remote_job(auth):
    return rest_client(
        credential=auth,
        subscription_id=os.environ.get(AZUREML_ARM_SUBSCRIPTION_KEY),
        base_url=os.environ.get(HISTORY_SERVICE_ENDPOINT_KEY))


def _get_data_asset_by_id(asset_id, storage_options=None):
    storage_options = _try_get_and_validate_workspace_info(storage_options)
    auth = AzureMLTokenAuthentication._initialize_aml_token_auth()
    if auth is not None:
        rest_client = _get_rest_client_in_remote_job(auth)
        return rest_client.data_version.get_by_asset_id(
            storage_options[STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION],
            storage_options[STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP],
            storage_options[STORAGE_OPTION_KEY_AZUREML_WORKSPACE],
            {'value': asset_id},
        )

    # parse asset_id for workspace location
    legacy_dataset_asset_id_pattern = re.compile(
        r'^azureml://locations/([^\/]+)/workspaces/([^\/]+)/data/([^\/]+)/versions/(.*)',
        re.IGNORECASE)
    legacy_dataset_asset_id_match = legacy_dataset_asset_id_pattern.match(asset_id)
    if not legacy_dataset_asset_id_match:
        raise ValueError(f'Missing required workspace location information in: `{asset_id}`')
    storage_options[STORAGE_OPTION_KEY_AZUREML_LOCATION] = legacy_dataset_asset_id_match.group(1)

    # this logic is only for enabling e2e test
    from azure.identity import AzureCliCredential
    auth = AzureCliCredential()
    client = _get_rest_client(storage_options, auth)
    asset = client.data_version.get_by_asset_id(
        storage_options[STORAGE_OPTION_KEY_AZUREML_SUBSCRIPTION],
        storage_options[STORAGE_OPTION_KEY_AZUREML_RESOURCEGROUP],
        storage_options[STORAGE_OPTION_KEY_AZUREML_WORKSPACE],
        {'value': asset_id},
    )
    return asset


def _get_data_asset_by_asset_uri(asset_uri_match, storage_options=None):
    subscription_id = asset_uri_match[0]
    resource_group = asset_uri_match[1]
    workspace_name = asset_uri_match[2]
    asset_name = asset_uri_match[3]
    asset_version = asset_uri_match[4]

    auth = AzureMLTokenAuthentication._initialize_aml_token_auth()
    # in remote job, get rest_client from env variables, no need to depend on sdk v2
    if auth is not None:
        rest_client = _get_rest_client_in_remote_job(auth)
        return rest_client.data_version.get(
            subscription_id,
            resource_group,
            workspace_name,
            asset_name,
            asset_version
        )

    from azure.identity import DefaultAzureCredential
    credential = DefaultAzureCredential()
    if storage_options is not None:
        credential = storage_options.get(STORAGE_OPTION_KEY_AZUREML_CREDENTIAL, DefaultAzureCredential())
    # else its local interactive scenario, calls sdk v2
    try:
        from azure.ai.ml import MLClient
    except ImportError:
        raise ImportError('The support of data asset uri requires dependency on azureml sdk v2 package,'
                          'please install package azure-ai-ml')

    ml_client = MLClient(credential, subscription_id, resource_group, workspace_name)
    return ml_client.jobs._dataset_dataplane_operations._operation.get(
        subscription_id,
        resource_group,
        workspace_name,
        asset_name,
        asset_version
    )
