import json
import os
from enum import Enum
from typing import Optional

import requests
from azureml.dataprep import DataPrepException
from azureml.dataprep.api._loggerfactory import _LoggerFactory

from ._constants import (AZURE_CLIENT_ID, IDENTITY_CLIENT_ID_ENV_NAME,
                         IDENTITY_INTERACT_TIMEOUT_ENV_NAME,
                         IDENTITY_TENANT_ID_ENV_NAME,
                         IDENTITY_USE_DEVICE_CODE_ENV_NAME)

_logger = None


def _get_logger():
    global _logger
    _logger = _logger or _LoggerFactory.get_logger(
        "dprep._azure_access_token_resolver")
    return _logger


def _print_and_log(message, is_error=False):
    print(message)
    if is_error:
        _get_logger().error(message)
    else:
        _get_logger().info(message)


class _IdentityType(Enum):
    MANAGED = 0
    SP = 1
    USER = 2


def _get_identity_type(sp, endpoint_type: Optional[str]):
    if sp is not None:
        return _IdentityType.SP  # Service Principal is in context ==> use SP identity

    is_obo_enabled = os.environ.get("AZUREML_OBO_ENABLED", "False") == 'True' or \
        os.environ.get(IDENTITY_USE_DEVICE_CODE_ENV_NAME, '').lower() == 'true'
    if is_obo_enabled:
        return _IdentityType.USER
    if "MSI_ENDPOINT" in os.environ or endpoint_type == 'Imds' or endpoint_type == 'MsiEndpoint':
        return _IdentityType.MANAGED  # MSI Endpoint is in context ==> Use MSI

    try:
        from azureml.core.run import Run
        from azureml.exceptions import RunEnvironmentException
    except ModuleNotFoundError:
        # There is no AzureML SDK presenting ==> Not in AzureML run context ==> use USER identity
        return _IdentityType.USER

    try:
        Run.get_context(allow_offline=False)
        return _IdentityType.MANAGED  # In AzureML run context ==> use MANAGED identity
    except RunEnvironmentException:
        pass
    except Exception as e:
        _print_and_log('Cannot determine which identity to use for data access due to exception {}. Fall back to use interactive login.'.format(
            e.__class__.__name__), is_error=True)

    return _IdentityType.USER  # Not in AzureML run context ==> use USER identity


def _raise_obo_error():
    class OboEndpointError(DataPrepException):
        def __init__(self):
            generic_message = 'No obo endpoint found in job environment variables.\n'
            message = generic_message + 'Missing env var: OBO_ENDPOINT'

            super().__init__(message, 'OboEndpoint', generic_message)

    raise OboEndpointError()


class NoIdentityFoundError(Exception):
    def __init__(self):
        super().__init__('No identity was found on compute.')


def _resolve_azure_access_token(scope, sp, client_id=None, endpoint_type=None, authority=None):
    """
    data access identity resolution:
    - if Workspace is signed in with a SP, then use SP identity for data access
    - else if in submitted run:
        - if env var "DEFAULT_IDENTITY_CLIENT_ID" is there (contract with AmlCompute), use UAI (User Assigned Identity)
        - else use SAI (System Assigned Identity)
    - else use user identity: browser login if possible otherwise device code login
    """
    logger = _get_logger()
    identity_type = _get_identity_type(sp, endpoint_type)
    _print_and_log('Resolving access token for scope "{}" using identity of type "{}".'.format(
        scope, identity_type.name))

    credential = None
    token = None
    expires_on = None
    if identity_type == _IdentityType.MANAGED:
        from azure.identity import ManagedIdentityCredential
        from azure.identity._credentials.app_service import \
            AppServiceCredential
        from azure.identity._credentials.imds import ImdsCredential

        # AML Compute will set IDENTITY_CLIENT_ID_ENV_NAME as environment variable to indicate which UAI to use.
        # When it is not set, SAI should be used
        client_id = client_id or os.environ.get(
            IDENTITY_CLIENT_ID_ENV_NAME, None)
        if client_id is None:
            _print_and_log('No identity was found on compute.')
            raise NoIdentityFoundError()

        _print_and_log(
            'Getting data access token with Assigned Identity (client_id={}) and endpoint type {}'.format(client_id, endpoint_type or "based on configuration"))

        # There are several ways to get access token for a given managed identity. Different Azure services provide
        # different mechanism to achieve this. For example, the MSI_ENDPOINT env var approach which we relied on
        # previously is App Services' way of exposing an endpoint for getting access token. IMDS is Azure VM
        # (also exposed by HOBO V2 in Batch) way of exposing an endpoint for getting access token.
        # The Azure Python Identity SDK, linked below, provides more insights into how it does fallback of various
        # managed identity approaches.
        if endpoint_type == 'Imds':
            credential = ImdsCredential(client_id=client_id)
        elif endpoint_type == 'MsiEndpoint':
            credential = AppServiceCredential(client_id=client_id)
        else:
            # https://github.com/Azure/azure-sdk-for-python/blob/40f3d19b92381699f348f354d3f4d0dc2df88bf0/sdk/identity/azure-identity/azure/identity/_credentials/managed_identity.py#L44
            credential = ManagedIdentityCredential(client_id=client_id)

        access_token = credential.get_token(scope)
        token = access_token.token
        expires_on = access_token.expires_on
    elif identity_type == _IdentityType.SP:
        from azure.identity import ClientSecretCredential
        sp_id = sp['servicePrincipalId']
        _print_and_log(
            'Getting data access token with Service Principal (id={}).'.format(sp_id))
        credential = ClientSecretCredential(
            sp['tenantId'], sp_id, sp['password'], authority=authority)
        access_token = credential.get_token(scope)
        token = access_token.token
        expires_on = access_token.expires_on
    elif identity_type == _IdentityType.USER:
        is_obo_enabled = os.environ.get("AZUREML_OBO_ENABLED", "False")
        if is_obo_enabled == "True":
            obo_endpoint = os.environ.get("OBO_ENDPOINT", None)
            if obo_endpoint:
                _print_and_log(
                    'Getting data access token using obo credentials.')
                normalized_scope = scope.split("/.default")[0]
                if not normalized_scope.endswith('/'):
                    normalized_scope += "/"
                token_auth_uri = "{}?resource={}".format(
                    obo_endpoint, normalized_scope)
                headers = {}
                # Add MSI_SECRET to the header, because in CI the OBO endpoint is actually MSI endpoint and expect MSI_SECRET
                secret = os.environ.get("MSI_SECRET")
                if secret:
                    headers["secret"] = secret
                resp = requests.get(token_auth_uri, headers=headers)
                resp_json = resp.json()
                token = resp_json['access_token']
                expires_on = resp_json['expires_on']
            else:
                _print_and_log(
                    'No obo endpoint found in job environment variables.')
                _raise_obo_error()
        else:
            from azure.core.exceptions import ClientAuthenticationError
            from azure.identity import (ChainedTokenCredential,
                                        DeviceCodeCredential,
                                        InteractiveBrowserCredential)
            tenant_id = os.environ.get(IDENTITY_TENANT_ID_ENV_NAME, None)
            always_use_device_code = os.environ.get(
                IDENTITY_USE_DEVICE_CODE_ENV_NAME, '').lower() == 'true'
            timeout_sec = 120
            try:
                timeout_sec = int(os.environ.get(
                    IDENTITY_INTERACT_TIMEOUT_ENV_NAME))
            except Exception:
                pass  # ignore invalid value
            message = 'Credentials are not provided to access data from the source. Please sign in using identity with required permission granted.' \
                '\nInteractive sign-in timeout: {} sec.'.format(timeout_sec)
            kwargs = dict(timeout=timeout_sec, authority=authority)
            if tenant_id:
                message += '\nCurrent sign-in tenant: {}.'.format(tenant_id)
                kwargs['tenant_id'] = tenant_id
            message += ('\nTo change the sign-in tenant, restart the session with tenant ID set to environment variable "{}" before sign in.'.format(IDENTITY_TENANT_ID_ENV_NAME) +
                        '\nTo always use device code for interactive sign-in, set environment variable "{}" to "true".'.format(IDENTITY_USE_DEVICE_CODE_ENV_NAME) +
                        '\nTo configure timeout, set environment variable "{}" to the number of seconds.'.format(IDENTITY_INTERACT_TIMEOUT_ENV_NAME))

            if always_use_device_code:
                credential = DeviceCodeCredential(AZURE_CLIENT_ID, **kwargs)
            else:
                credential = ChainedTokenCredential(InteractiveBrowserCredential(
                    **kwargs), DeviceCodeCredential(AZURE_CLIENT_ID, **kwargs))

            logger.info('Using interactive sign-in: tenant_id={}, timeoust_sec={}, always_use_device_code={}'.format(
                tenant_id, timeout_sec, always_use_device_code))

            try:
                access_token = credential.get_token(scope)
                token = access_token.token
                expires_on = access_token.expires_on
            except ClientAuthenticationError as ex:
                if '{}: Timed out after waiting {} seconds for the user to authenticate'.format(InteractiveBrowserCredential.__name__, timeout_sec) in ex.message \
                        and DeviceCodeCredential.__name__ not in ex.message:
                    # ChainedTokenCredential does not fallback to the next for timeout case.
                    # But this might be due to user not being able to see the browser page.
                    # So we should attempt DeviceCodeCredential here.
                    _get_logger().info('Attempt device code sign-in due to browser interactive sign-in timeout')
                    print(
                        '\nFailed to sign in with interactive browser. Falling back to device code...\n')
                    credential = DeviceCodeCredential(
                        AZURE_CLIENT_ID, **kwargs)
                    access_token = credential.get_token(scope)
                    token = access_token.token
                    expires_on = access_token.expires_on
                else:
                    raise
    else:
        logger.error('Unknown identity type "{}"'.format(identity_type.name))
        raise ValueError(
            'Unknown identity type "{}"'.format(identity_type.name))

    logger.info('Succeeded to resolve access token for scope "{}" using identity of type "{}".'.format(
        scope, identity_type.name))
    return (token, expires_on)


def register_access_token_resolver(requests_channel):
    def resolve(request, writer, socket):
        try:
            logger = _get_logger()
            scope = request.get('scope')
            sp = request.get('sp')
            authority = request.get('authority')
            sp_cred = json.loads(sp) if sp is not None else None
            (token, expires_on) = _resolve_azure_access_token(scope, sp_cred, authority = authority)

            writer.write(json.dumps({
                'result': 'success',
                'token': token,
                'seconds': expires_on
            }))

        except NoIdentityFoundError:
            writer.write(json.dumps(
                {'result': 'error', 'error': 'NO_IDENTITY_FOUND_ON_COMPUTE'}))
        except Exception as e:
            try:
                identity_type = _get_identity_type(sp, None).name
            except:
                identity_type = 'Unable to determine'
            print('Failed to get data access token for scope "{}" due to exception:\n{}.'.format(
                scope, e))

            logger.error('Failed to get data access token for scope "{}" using identity of type "{}" due to exception {}.'.format(
                scope, identity_type, e.__class__.__name__))
            writer.write(json.dumps({'result': 'error', 'error': str(e)}))

    requests_channel.register_handler('resolve_azure_access_token', resolve)
