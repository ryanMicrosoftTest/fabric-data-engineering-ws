# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
# pylint: disable=line-too-long

from pprint import pformat

# Add names of clouds that don't allow telemetry data collection here such as some air-gapped clouds.
CLOUDS_FORBIDDING_TELEMETRY = ['USSec', 'USNat']

# Add names of clouds that don't allow Aladdin requests for command recommendations here
CLOUDS_FORBIDDING_ALADDIN_REQUEST = ['USSec', 'USNat']

# We can source the cloud name from the environment as well if not passed explicitly
AZUREML_CLOUD_ENV_NAME = "AZUREML_CURRENT_CLOUD"


class CloudNotRegisteredException(Exception):
    def __init__(self, cloud_name):
        super(CloudNotRegisteredException, self).__init__(cloud_name)
        self.cloud_name = cloud_name

    def __str__(self):
        return "The cloud '{}' is not registered.".format(self.cloud_name)


class CloudAlreadyRegisteredException(Exception):
    def __init__(self, cloud_name):
        super(CloudAlreadyRegisteredException, self).__init__(cloud_name)
        self.cloud_name = cloud_name

    def __str__(self):
        return "The cloud '{}' is already registered.".format(self.cloud_name)


class CannotUnregisterCloudException(Exception):
    pass


class CloudEndpointNotSetException(Exception):
    pass


class CloudSuffixNotSetException(Exception):
    pass


class CloudEndpoints:  # pylint: disable=too-few-public-methods,too-many-instance-attributes

    def __init__(self,  # pylint: disable=unused-argument
                 management=None,
                 resource_manager=None,
                 sql_management=None,
                 batch_resource_id=None,
                 gallery=None,
                 active_directory=None,
                 active_directory_resource_id=None,
                 active_directory_graph_resource_id=None,
                 microsoft_graph_resource_id=None,
                 active_directory_data_lake_resource_id=None,
                 vm_image_alias_doc=None,
                 media_resource_id=None,
                 ossrdbms_resource_id=None,
                 log_analytics_resource_id=None,
                 app_insights_resource_id=None,
                 app_insights_telemetry_channel_resource_id=None,
                 synapse_analytics_resource_id=None,
                 attestation_resource_id=None,
                 portal=None,
                 azmirror_storage_account_resource_id=None,
                 **kwargs):  # To support init with __dict__ for deserialization
        # Attribute names are significant. They are used when storing/retrieving clouds from config
        self.management = management
        self.resource_manager = resource_manager
        self.sql_management = sql_management
        self.batch_resource_id = batch_resource_id
        self.gallery = gallery
        self.active_directory = active_directory
        self.active_directory_resource_id = active_directory_resource_id
        self.active_directory_graph_resource_id = active_directory_graph_resource_id
        self.microsoft_graph_resource_id = microsoft_graph_resource_id
        self.active_directory_data_lake_resource_id = active_directory_data_lake_resource_id
        self.vm_image_alias_doc = vm_image_alias_doc
        self.media_resource_id = media_resource_id
        self.ossrdbms_resource_id = ossrdbms_resource_id
        self.log_analytics_resource_id = log_analytics_resource_id
        self.app_insights_resource_id = app_insights_resource_id
        self.app_insights_telemetry_channel_resource_id = app_insights_telemetry_channel_resource_id
        self.synapse_analytics_resource_id = synapse_analytics_resource_id
        self.attestation_resource_id = attestation_resource_id
        self.portal = portal
        self.azmirror_storage_account_resource_id = azmirror_storage_account_resource_id

    def has_endpoint_set(self, endpoint_name):
        try:
            # Can't simply use hasattr here as we override __getattribute__ below.
            # Python 3 hasattr() only returns False if an AttributeError is raised but we raise
            # CloudEndpointNotSetException. This exception is not a subclass of AttributeError.
            getattr(self, endpoint_name)
            return True
        except Exception:  # pylint: disable=broad-except
            return False

    def __getattribute__(self, name):
        val = object.__getattribute__(self, name)
        return val


class CloudSuffixes:  # pylint: disable=too-few-public-methods,too-many-instance-attributes

    def __init__(self,  # pylint: disable=unused-argument
                 storage_endpoint=None,
                 storage_sync_endpoint=None,
                 keyvault_dns=None,
                 mhsm_dns=None,
                 sql_server_hostname=None,
                 azure_datalake_store_file_system_endpoint=None,
                 azure_datalake_analytics_catalog_and_job_endpoint=None,
                 acr_login_server_endpoint=None,
                 mysql_server_endpoint=None,
                 postgresql_server_endpoint=None,
                 mariadb_server_endpoint=None,
                 synapse_analytics_endpoint=None,
                 attestation_endpoint=None,
                 **kwargs):  # To support init with __dict__ for deserialization
        # Attribute names are significant. They are used when storing/retrieving clouds from config
        self.storage_endpoint = storage_endpoint
        self.storage_sync_endpoint = storage_sync_endpoint
        self.keyvault_dns = keyvault_dns
        self.mhsm_dns = mhsm_dns
        self.sql_server_hostname = sql_server_hostname
        self.mysql_server_endpoint = mysql_server_endpoint
        self.postgresql_server_endpoint = postgresql_server_endpoint
        self.mariadb_server_endpoint = mariadb_server_endpoint
        self.azure_datalake_store_file_system_endpoint = azure_datalake_store_file_system_endpoint
        self.azure_datalake_analytics_catalog_and_job_endpoint = azure_datalake_analytics_catalog_and_job_endpoint
        self.acr_login_server_endpoint = acr_login_server_endpoint
        self.synapse_analytics_endpoint = synapse_analytics_endpoint
        self.attestation_endpoint = attestation_endpoint

    def __getattribute__(self, name):
        val = object.__getattribute__(self, name)
        return val

class Cloud:  # pylint: disable=too-few-public-methods
    """ Represents an Azure Cloud instance """

    def __init__(self,
                 name,
                 endpoints=None,
                 suffixes=None,
                 profile=None,
                 is_active=False):
        self.name = name
        self.endpoints = endpoints or CloudEndpoints()
        self.suffixes = suffixes or CloudSuffixes()
        self.profile = profile
        self.is_active = is_active

    def __str__(self):
        o = {
            'profile': self.profile,
            'name': self.name,
            'is_active': self.is_active,
            'endpoints': vars(self.endpoints),
            'suffixes': vars(self.suffixes),
        }
        return pformat(o)

    def to_json(self):
        return {'name': self.name, "endpoints": self.endpoints.__dict__, "suffixes": self.suffixes.__dict__}

    @classmethod
    def from_json(cls, json_str):
        return cls(json_str['name'],
                   endpoints=CloudEndpoints(**json_str['endpoints']),
                   suffixes=CloudSuffixes(**json_str['suffixes']))


AZURE_PUBLIC_CLOUD = Cloud(
    'AzureCloud',
    endpoints=CloudEndpoints(
        management='https://management.core.windows.net/',
        resource_manager='https://management.azure.com/',
        sql_management='https://management.core.windows.net:8443/',
        batch_resource_id='https://batch.core.windows.net/',
        gallery='https://gallery.azure.com/',
        active_directory='https://login.microsoftonline.com',
        active_directory_resource_id='https://management.core.windows.net/',
        active_directory_graph_resource_id='https://graph.windows.net/',
        microsoft_graph_resource_id='https://graph.microsoft.com/',
        active_directory_data_lake_resource_id='https://datalake.azure.net/',
        vm_image_alias_doc='https://raw.githubusercontent.com/Azure/azure-rest-api-specs/master/arm-compute/quickstart-templates/aliases.json',
        media_resource_id='https://rest.media.azure.net',
        ossrdbms_resource_id='https://ossrdbms-aad.database.windows.net',
        app_insights_resource_id='https://api.applicationinsights.io',
        log_analytics_resource_id='https://api.loganalytics.io',
        app_insights_telemetry_channel_resource_id='https://dc.applicationinsights.azure.com/v2/track',
        synapse_analytics_resource_id='https://dev.azuresynapse.net',
        attestation_resource_id='https://attest.azure.net',
        portal='https://portal.azure.com'),
    suffixes=CloudSuffixes(
        storage_endpoint='core.windows.net',
        storage_sync_endpoint='afs.azure.net',
        keyvault_dns='.vault.azure.net',
        mhsm_dns='.managedhsm.azure.net',
        sql_server_hostname='.database.windows.net',
        mysql_server_endpoint='.mysql.database.azure.com',
        postgresql_server_endpoint='.postgres.database.azure.com',
        mariadb_server_endpoint='.mariadb.database.azure.com',
        azure_datalake_store_file_system_endpoint='azuredatalakestore.net',
        azure_datalake_analytics_catalog_and_job_endpoint='azuredatalakeanalytics.net',
        acr_login_server_endpoint='.azurecr.io',
        synapse_analytics_endpoint='.dev.azuresynapse.net',
        attestation_endpoint='.attest.azure.net'))

AZURE_CHINA_CLOUD = Cloud(
    'AzureChinaCloud',
    endpoints=CloudEndpoints(
        management='https://management.core.chinacloudapi.cn/',
        resource_manager='https://management.chinacloudapi.cn',
        sql_management='https://management.core.chinacloudapi.cn:8443/',
        batch_resource_id='https://batch.chinacloudapi.cn/',
        gallery='https://gallery.chinacloudapi.cn/',
        active_directory='https://login.chinacloudapi.cn',
        active_directory_resource_id='https://management.core.chinacloudapi.cn/',
        active_directory_graph_resource_id='https://graph.chinacloudapi.cn/',
        microsoft_graph_resource_id='https://microsoftgraph.chinacloudapi.cn',
        vm_image_alias_doc='https://raw.githubusercontent.com/Azure/azure-rest-api-specs/master/arm-compute/quickstart-templates/aliases.json',
        media_resource_id='https://rest.media.chinacloudapi.cn',
        ossrdbms_resource_id='https://ossrdbms-aad.database.chinacloudapi.cn',
        app_insights_resource_id='https://api.applicationinsights.azure.cn',
        log_analytics_resource_id='https://api.loganalytics.azure.cn',
        app_insights_telemetry_channel_resource_id='https://dc.applicationinsights.azure.cn/v2/track',
        synapse_analytics_resource_id='https://dev.azuresynapse.azure.cn',
        portal='https://portal.azure.cn'),
    suffixes=CloudSuffixes(
        storage_endpoint='core.chinacloudapi.cn',
        keyvault_dns='.vault.azure.cn',
        mhsm_dns='.managedhsm.azure.cn',
        sql_server_hostname='.database.chinacloudapi.cn',
        mysql_server_endpoint='.mysql.database.chinacloudapi.cn',
        postgresql_server_endpoint='.postgres.database.chinacloudapi.cn',
        mariadb_server_endpoint='.mariadb.database.chinacloudapi.cn',
        acr_login_server_endpoint='.azurecr.cn',
        synapse_analytics_endpoint='.dev.azuresynapse.azure.cn'))

AZURE_US_GOV_CLOUD = Cloud(
    'AzureUSGovernment',
    endpoints=CloudEndpoints(
        management='https://management.core.usgovcloudapi.net/',
        resource_manager='https://management.usgovcloudapi.net/',
        sql_management='https://management.core.usgovcloudapi.net:8443/',
        batch_resource_id='https://batch.core.usgovcloudapi.net/',
        gallery='https://gallery.usgovcloudapi.net/',
        active_directory='https://login.microsoftonline.us',
        active_directory_resource_id='https://management.core.usgovcloudapi.net/',
        active_directory_graph_resource_id='https://graph.windows.net/',
        microsoft_graph_resource_id='https://graph.microsoft.us/',
        vm_image_alias_doc='https://raw.githubusercontent.com/Azure/azure-rest-api-specs/master/arm-compute/quickstart-templates/aliases.json',
        media_resource_id='https://rest.media.usgovcloudapi.net',
        ossrdbms_resource_id='https://ossrdbms-aad.database.usgovcloudapi.net',
        app_insights_resource_id='https://api.applicationinsights.us',
        log_analytics_resource_id='https://api.loganalytics.us',
        app_insights_telemetry_channel_resource_id='https://dc.applicationinsights.us/v2/track',
        synapse_analytics_resource_id='https://dev.azuresynapse.usgovcloudapi.net',
        portal='https://portal.azure.us'),
    suffixes=CloudSuffixes(
        storage_endpoint='core.usgovcloudapi.net',
        storage_sync_endpoint='afs.azure.us',
        keyvault_dns='.vault.usgovcloudapi.net',
        mhsm_dns='.managedhsm.usgovcloudapi.net',
        sql_server_hostname='.database.usgovcloudapi.net',
        mysql_server_endpoint='.mysql.database.usgovcloudapi.net',
        postgresql_server_endpoint='.postgres.database.usgovcloudapi.net',
        mariadb_server_endpoint='.mariadb.database.usgovcloudapi.net',
        acr_login_server_endpoint='.azurecr.us',
        synapse_analytics_endpoint='.dev.azuresynapse.usgovcloudapi.net'))

AZURE_GERMAN_CLOUD = Cloud(
    'AzureGermanCloud',
    endpoints=CloudEndpoints(
        management='https://management.core.cloudapi.de/',
        resource_manager='https://management.microsoftazure.de',
        sql_management='https://management.core.cloudapi.de:8443/',
        batch_resource_id='https://batch.cloudapi.de/',
        gallery='https://gallery.cloudapi.de/',
        active_directory='https://login.microsoftonline.de',
        active_directory_resource_id='https://management.core.cloudapi.de/',
        active_directory_graph_resource_id='https://graph.cloudapi.de/',
        microsoft_graph_resource_id='https://graph.microsoft.de',
        vm_image_alias_doc='https://raw.githubusercontent.com/Azure/azure-rest-api-specs/master/arm-compute/quickstart-templates/aliases.json',
        media_resource_id='https://rest.media.cloudapi.de',
        ossrdbms_resource_id='https://ossrdbms-aad.database.cloudapi.de',
        portal='https://portal.microsoftazure.de'),
    suffixes=CloudSuffixes(
        storage_endpoint='core.cloudapi.de',
        keyvault_dns='.vault.microsoftazure.de',
        mhsm_dns='.managedhsm.microsoftazure.de',
        sql_server_hostname='.database.cloudapi.de',
        mysql_server_endpoint='.mysql.database.cloudapi.de',
        postgresql_server_endpoint='.postgres.database.cloudapi.de',
        mariadb_server_endpoint='.mariadb.database.cloudapi.de'))

HARD_CODED_CLOUD_LIST = [AZURE_PUBLIC_CLOUD, AZURE_CHINA_CLOUD, AZURE_US_GOV_CLOUD, AZURE_GERMAN_CLOUD]