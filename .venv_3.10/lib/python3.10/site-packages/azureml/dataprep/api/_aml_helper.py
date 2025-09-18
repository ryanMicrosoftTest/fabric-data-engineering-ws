from .engineapi.engine import CancellationToken
import os
from typing import Callable
import re

_long_form_aml_uri = re.compile(
    r'^azureml://subscriptions/([^\/]+)/resourcegroups/([^\/]+)/'
    r'(?:providers/Microsoft.MachineLearningServices/)?workspaces/([^\/]+)/(.*)',
    re.IGNORECASE)


class _AmlEnvironmentVariables:
    _tracked_env_vars = ['AZUREML_RUN_TOKEN', 'AZUREML_ARM_SUBSCRIPTION', 'AZUREML_ARM_RESOURCEGROUP',
                         'AZUREML_ARM_WORKSPACE_NAME', 'AZUREML_SERVICE_ENDPOINT', 'AZUREML_ARM_PROJECT_NAME',
                         'AZUREML_RUN_ID']

    def __init__(self):
        self._existing_values = {}
        for var in _AmlEnvironmentVariables._tracked_env_vars:
            self._existing_values[var] = os.environ.get(var)

    def get_changed(self) -> dict:
        changed = {}
        for var in _AmlEnvironmentVariables._tracked_env_vars:
            existing = self._existing_values[var]
            new = os.environ.get(var)
            if existing != new:
                self._existing_values[var] = new
                changed[var] = new
        return changed


def update_aml_env_vars(engine_api_func: Callable[[], 'EngineAPI']):
    def decorator(send_message_func: Callable[[str, object, CancellationToken], object]):
        from functools import wraps

        aml_env_vars = _AmlEnvironmentVariables()

        @wraps(send_message_func)
        def wrapper(op_code: str, message: object = None, cancellation_token: CancellationToken = None) -> object:
            changed = aml_env_vars.get_changed()
            if len(changed) > 0:
                engine_api_func().update_environment_variable(changed)
            return send_message_func(op_code, message, cancellation_token)

        return wrapper
    return decorator


def get_workspace_from_run():
    from azureml.core import Run
    try:
        return Run.get_context().experiment.workspace
    except AttributeError:
        return None


def verify_workspace(workspace, subscription_id, resource_group, workspace_name):
    if workspace.subscription_id.lower() != subscription_id.lower():
        raise RuntimeError('Non-matching subscription ID. '
            'Requested: "{}" vs Actual: "{}"'.format(workspace.subscription_id, subscription_id))
    if workspace.resource_group.lower() != resource_group.lower():
        raise RuntimeError('Non-matching resource group. '
            'Requested: "{}" vs Actual: "{}"'.format(workspace.resource_group, resource_group))
    # workspace name is case insensitive
    if workspace.name.lower() != workspace_name.lower():
        raise RuntimeError('Non-matching workspace name. '
            'Requested: "{}" vs Actual: "{}"'.format(workspace.name, workspace_name))


# utility function added to share in mltable/fsspec
def _parse_workspace_context_from_longform_uri(uri):
    long_form_uri_match = _long_form_aml_uri.match(uri)

    if long_form_uri_match:
        return {
            'subscription': long_form_uri_match.group(1),
            'resource_group': long_form_uri_match.group(2),
            'workspace_name': long_form_uri_match.group(3)
        }

    return None
