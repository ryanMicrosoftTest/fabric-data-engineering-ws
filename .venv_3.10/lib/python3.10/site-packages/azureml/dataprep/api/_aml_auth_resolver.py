import json
import os

from azureml.dataprep.api._loggerfactory import _LoggerFactory

from ._aml_helper import get_workspace_from_run, verify_workspace


logger = None

def get_logger():
    global logger
    if logger is not None:
        return logger

    logger = _LoggerFactory.get_logger("dprep._aml_auth_resolver")
    return logger


class WorkspaceContextCache:
    _cache = {}

    @staticmethod
    def add(workspace):
        try:
            key = WorkspaceContextCache._get_key(workspace.subscription_id, workspace.resource_group, workspace.name)
            WorkspaceContextCache._cache[key] = workspace
        except Exception as e:
            get_logger().info('Cannot cache workspace due to: {}'.format(repr(e)))

    @staticmethod
    def get(subscription_id, resource_group_name, workspace_name):
        try:
            key = WorkspaceContextCache._get_key(subscription_id, resource_group_name, workspace_name)
            workspace = WorkspaceContextCache._cache[key]
            return workspace
        except Exception as e:
            get_logger().info('Cannot find cached workspace due to: {}'.format(repr(e)))
            return None

    @staticmethod
    def _get_key(subscription_id, resource_group_name, workspace_name):
        return ''.join([subscription_id, resource_group_name, workspace_name])


def _resolve_auth_from_workspace(subscription, resource_group, ws_name, auth_type, creds):
    try:
        from azureml.core import Workspace
        from azureml._base_sdk_common.service_discovery import get_service_url
    except ImportError:
        # azureml-core is not installed try aml run token auth for remote aml run
        from ._aml_auth._azureml_token_authentication import AzureMLTokenAuthentication
        aml_auth = AzureMLTokenAuthentication._initialize_aml_token_auth()

        if aml_auth:
            get_logger().info('azureml-core is not installed, AML run token auth returned successfully')
            return \
                {
                    'service_endpoint': os.environ.get('AZUREML_SERVICE_ENDPOINT'),
                    'auth': aml_auth.get_authentication_header()
                }
        else:
            # this is a local job experience, use DefaultAzureCredential and get service endpoint from sdkv2
            get_logger().info('azureml-core is not installed and AML run token auth is None,'
                              'trying to get service endpoint from sdkv2')
            try:
                from azure.ai.ml import MLClient
            except ImportError:
                raise ImportError('The support of datastore requires dependency on azureml sdk v2 package,'
                                  'please install package azure-ai-ml')

            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential()
            ml_client = MLClient(credential, subscription, resource_group, ws_name)

            return \
                {
                    'service_endpoint': ml_client.jobs._api_url,
                    'auth': {
                                "Authorization":
                                # get arm scope token
                                # getting arm resource url from ml_client which already handles different clouds
                                "Bearer " + credential.get_token(f'{ml_client._base_url}/.default').token
                    }
                }

    if not ws_name or not subscription or not resource_group:
        raise ValueError('Invalid workspace details.')

    ws = WorkspaceContextCache.get(subscription, resource_group, ws_name)

    if not ws:
        ws = get_workspace_from_run()

    if not ws:
        auth = _get_auth(creds, auth_type)
        ws = Workspace.get(ws_name, auth=auth, subscription_id=subscription, resource_group=resource_group)

    verify_workspace(ws, subscription, resource_group, ws_name)

    try:
        host = os.environ.get('AZUREML_SERVICE_ENDPOINT') or \
               get_service_url(ws._auth, ws.service_context._get_workspace_scope(), ws._workspace_id,
                               ws.discovery_url)
    except AttributeError:
        # This check is for backward compatibility, handling cases where azureml-core package is pre-Feb2020,
        # as ws.discovery_url was added in this PR:
        # https://msdata.visualstudio.com/Vienna/_git/AzureMlCli/pullrequest/310794
        host = get_service_url(ws._auth, ws.service_context._get_workspace_scope(), ws._workspace_id)

    return {'service_endpoint': host, 'auth': ws._auth.get_authentication_header()}


def register_datastore_resolver(requests_channel):
    def resolve(request, writer, socket):
        from azureml.exceptions import RunEnvironmentException

        try:
            auth_type = request.get('auth_type')
            ws_name = request.get('ws_name')
            subscription = request.get('subscription')
            resource_group = request.get('resource_group')
            extra_args = json.loads(request.get('extra_args') or '{}')

            auth_info = _resolve_auth_from_workspace(subscription, resource_group, ws_name, auth_type, extra_args)
            writer.write(json.dumps({
                'result': 'success',
                'auth': json.dumps(auth_info['auth']),
                'host': auth_info['service_endpoint']
            }))
        except ValueError:
            writer.write(json.dumps({'result': 'error', 'error': 'InvalidWorkspace'}))
        except RunEnvironmentException as e:
            writer.write(json.dumps({
                'result': 'error',
                'error': 'Exception trying to get workspace information from the run. Error: {}'.format(e.message)
            }))
        except Exception as e:
            writer.write(json.dumps({'result': 'error', 'error': str(e)}))

    requests_channel.register_handler('resolve_auth_from_workspace', resolve)


def _get_auth(creds, auth_type):
    from azureml.core.authentication import InteractiveLoginAuthentication, AzureCliAuthentication, \
        ServicePrincipalAuthentication
    from azureml.core import VERSION
    import azureml.dataprep as dprep

    def log_version_issues(exception):
        log.warning(
            "Failed to construct auth object. Exception : {}, AML Version: {}, DataPrep Version: {}".format(
                type(exception).__name__, VERSION, dprep.__version__
            )
        )

    log = get_logger()
    cloud = creds.get('cloudType')
    tenant_id = creds.get('tenantId')
    auth_class = creds.get('authClass')

    if auth_type == 'SP':
        if not creds or not tenant_id:
            raise ValueError("InvalidServicePrincipalCreds")
        try:
            return ServicePrincipalAuthentication(tenant_id, creds['servicePrincipalId'],
                                                  creds['password'], cloud=creds['cloudType'])
        except Exception as e:
            log_version_issues(e)
            return ServicePrincipalAuthentication(tenant_id, creds['servicePrincipalId'], creds['password'])

    if auth_class == AzureCliAuthentication.__name__:
        try:
            return AzureCliAuthentication(cloud=cloud)
        except Exception as e:
            log_version_issues(e)
            return AzureCliAuthentication()

    # fallback to interactive authentication which has internal authentication fallback
    try:
        return InteractiveLoginAuthentication(tenant_id=tenant_id, cloud=cloud)
    except Exception as e:
        log_version_issues(e)
        return InteractiveLoginAuthentication()
