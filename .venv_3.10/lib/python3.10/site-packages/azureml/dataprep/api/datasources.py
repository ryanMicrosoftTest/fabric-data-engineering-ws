# Copyright (c) Microsoft Corporation. All rights reserved.
from ._vendor.cloud import HARD_CODED_CLOUD_LIST, AZURE_PUBLIC_CLOUD
from .engineapi.typedefinitions import (DataSourceTarget, AzureBlobResourceDetails, DataSourcePropertyValue,
                                        ResourceDetails, LocalResourceDetails, HttpResourceDetails,
                                        AzureDataLakeResourceDetails,
                                        Secret as SecretId, OutputFilePropertyValue, DatabaseType, DatabaseAuthType,
                                        OutputFileDestination, ADLSGen2ResourceDetails, DatabaseSslMode)
from .secretmanager import register_secret, create_secret
from ._oauthToken import get_az_cli_tokens
from ._datastore_helper import _set_clould_type
from typing import TypeVar, List, Callable, cast
from urllib.parse import urlparse
import os
import re
import json


Path = TypeVar('Path', str, List[str])
Secret = TypeVar('Secret', str, SecretId)


def _extract_resource_details(path: Path, extractor: Callable[[str], ResourceDetails]) -> List[ResourceDetails]:
    return [extractor(path)] if isinstance(path, str) else [extractor(p) for p in path]


def _extract_local_resource_details(path: str) -> ResourceDetails:
    return cast(ResourceDetails, LocalResourceDetails(os.path.abspath(os.path.expanduser(path))))


def _extract_http_resource_details(path: str) -> ResourceDetails:
    return cast(ResourceDetails, HttpResourceDetails(path))


def _extract_blob_resource_details(path: str) -> ResourceDetails:
    parts_list = urlparse(path)
    path = '{0}://{1}{2}'.format(*parts_list)
    query_string = parts_list.query or None
    if query_string:
        register_secret(value=query_string, id=path)
        resource_details = AzureBlobResourceDetails(path, create_secret(path))
    else:
        resource_details = AzureBlobResourceDetails(path)
    return cast(ResourceDetails, resource_details)


storage_suffix = r"({})".format("|".join([re.escape(cloud.suffixes.storage_endpoint) for cloud in HARD_CODED_CLOUD_LIST]))
blob_pattern = re.compile(r'^https?://[^/]+\.blob\.{}'.format(storage_suffix), re.IGNORECASE)
wasb_pattern = re.compile(r'^wasbs?://', re.IGNORECASE)
adls_pattern = re.compile(r'^adl://[^/]+\.azuredatalake(store)?\.net', re.IGNORECASE)
adlsgen2_pattern = re.compile(r'^https?://[^/]+\.dfs\.{}'.format(storage_suffix), re.IGNORECASE)
abfs_pattern = re.compile(r'^abfss?://', re.IGNORECASE)
http_pattern = re.compile(r'^https?://', re.IGNORECASE)


class FileDataSource:
    def __init__(self, value: DataSourcePropertyValue):
        self.underlying_value = value

    @staticmethod
    def datasource_from_str(path: Path) -> 'FileDataSource':        
        def to_datasource(p: str):
            if blob_pattern.match(p) or wasb_pattern.match(p):
                return BlobDataSource(p)
            elif adls_pattern.match(p):
                return DataLakeDataSource(p)
            elif adlsgen2_pattern.match(p) or abfs_pattern.match(p):
                return ADLSGen2(p)
            elif http_pattern.match(p):
                return HttpDataSource(p)
            else:
                return LocalDataSource(p)

        def get_cloud(p: str):
            parse_result = urlparse(p)
            for cloud in HARD_CODED_CLOUD_LIST:
                if parse_result.hostname.endswith(cloud.suffixes.storage_endpoint):
                    return cloud
            return AZURE_PUBLIC_CLOUD

        if isinstance(path, list):
            if len(path) == 0:
                raise ValueError('No paths were provided.')

            data_sources = [to_datasource(p) for p in path]
            cls = type(data_sources[0])
            if not all(isinstance(s, cls) for s in data_sources):
                raise ValueError('Found paths of multiple types (Local, Blob, ADLS, etc). Please specify paths of a single type.')
            if cls is BlobDataSource or cls is ADLSGen2:
                clouds = [get_cloud(p) for p in path]
                if not all(c.name == clouds[0].name for c in clouds):
                    raise ValueError('Found paths of multiple cloudss. Please specify paths of a single cloud.')
                _set_clould_type(clouds[0])

            return cls(path)
        elif isinstance(path, str):
            if len(path) == 0:
                raise ValueError('The path provided was empty. Please specify a valid path to the file to read.')

            datasource = to_datasource(path)
            if isinstance(datasource, (BlobDataSource, ADLSGen2)):
                _set_clould_type(get_cloud(path))
            return datasource
        else:
            raise ValueError('Unsupported path. Expected str or List[str].')


class LocalDataSource(FileDataSource):
    """
    Describes a source of data that is available from local disk.

    :param path: Path to file(s) or folder. Can be absolute or relative.
    """
    def __init__(self, path: Path):
        resource_details = _extract_resource_details(path, _extract_local_resource_details)
        datasource_property = DataSourcePropertyValue(target=DataSourceTarget.LOCAL, resource_details=resource_details)
        super().__init__(datasource_property)


class HttpDataSource(FileDataSource):
    """
    Describes a source of data that is available from http or https.

    :param path: URL to the file.
    """
    def __init__(self, path: Path):
        resource_details = _extract_resource_details(path, _extract_http_resource_details)
        datasource_property = DataSourcePropertyValue(target=DataSourceTarget.HTTP, resource_details=resource_details)
        super().__init__(datasource_property)


class BlobDataSource(FileDataSource):
    """
    Describes a source of data that is available from Azure Blob Storage.

    :param path: URL of the file(s) or folder in Azure Blob Storage.
    """
    def __init__(self, path: Path):
        resource_details = _extract_resource_details(path, _extract_blob_resource_details)
        datasource_property = DataSourcePropertyValue(target=DataSourceTarget.AZUREBLOBSTORAGE,
                                                      resource_details=resource_details)
        super().__init__(datasource_property)


class _DataLakeCredentialEncoder:
    @staticmethod
    def _register_secret(secret_value: str = None) -> Callable[[str], ResourceDetails]:
        def _path_to_resource_details(path: str) -> ResourceDetails:
            secret = None
            if secret_value:
                register_secret(value=secret_value, id=path)
                secret = create_secret(path)
            resource_details = AzureDataLakeResourceDetails(secret, path)
            return cast(ResourceDetails, resource_details)
        return _path_to_resource_details

    @staticmethod
    def _encode_token(access_token: str, refresh_token: str) -> str:
        payload = {'accessToken': access_token, 'type': 'oauth'}
        if refresh_token:
            payload['refreshToken'] = refresh_token
        return json.dumps(payload)


class DataLakeDataSource(FileDataSource):
    """
    Describes a source of data that is available from Azure Data Lake.

    :param path: URL of the file(s) or folder in Azure Data Lake.
    :param access_token: (Optional) Access token.
    :param refresh_token: (Optional) Refresh token.
    :param tenant: (Optional) Tenant ID.
    """

    def __init__(self, path: Path, access_token: str = None, refresh_token: str = None, tenant: str = None):
        secret_value = None
        if access_token is not None or tenant is not None:
            if access_token is None and refresh_token is None:
                access, refresh = get_az_cli_tokens(tenant)
                secret_value = _DataLakeCredentialEncoder._encode_token(access, refresh)
            else:
                secret_value = _DataLakeCredentialEncoder._encode_token(access_token, refresh_token)
        resource_details = _extract_resource_details(path, _DataLakeCredentialEncoder._register_secret(secret_value))
        datasource_property = DataSourcePropertyValue(target=DataSourceTarget.AZUREDATALAKESTORAGE,
                                                      resource_details=resource_details)
        super().__init__(datasource_property)


class _ADLSGen2CredentialEncoder:
    @staticmethod
    def _register_secret(secret_value: str = None) -> Callable[[str], ResourceDetails]:
        def _path_to_resource_details(path: str) -> ResourceDetails:
            secret = None
            if secret_value:
                register_secret(value=secret_value, id=path)
                secret = create_secret(path)
            resource_details = ADLSGen2ResourceDetails(secret, path)
            return cast(ResourceDetails, resource_details)
        return _path_to_resource_details

    @staticmethod
    def _encode_token(access_token: str, refresh_token: str) -> str:
        payload = {'accessToken': access_token, 'type': 'oauth'}
        if refresh_token:
            payload['refreshToken'] = refresh_token
        return json.dumps(payload)


class ADLSGen2(FileDataSource):
    """
    Describes a source of data that is available from ADLSGen2.

    :param path: URL of the file(s) or folder in ADLSGen2.
    :param access_token: (Optional) Access token.
    :param refresh_token: (Optional) Refresh token.
    :param tenant: (Optional) Tenant ID.
    """

    def __init__(self, path: Path, access_token: str = None, refresh_token: str = None, tenant: str = None):
        secret_value = None
        if access_token is not None or tenant is not None:
            if access_token is None and refresh_token is None:
                access, refresh = get_az_cli_tokens(tenant)
                secret_value = _ADLSGen2CredentialEncoder._encode_token(access, refresh)
            else:
                secret_value = _DataLakeCredentialEncoder._encode_token(access_token, refresh_token)
        resource_details = _extract_resource_details(path, _ADLSGen2CredentialEncoder._register_secret(secret_value))
        datasource_property = DataSourcePropertyValue(target=DataSourceTarget.ADLSGEN2,
                                                      resource_details=resource_details)
        super().__init__(datasource_property)


class MSSQLDataSource:
    """
    Represents a datasource that points to a Microsoft SQL Database.

    :var server_name: The SQL Server name.
    :vartype server: str
    :var database_name: The database name.
    :vartype database: str
    :var user_name: The username used for logging into the database.
    :vartype user_name: str
    :var password: The password used for logging into the database.
    :vartype password: str
    :var trust_server: Trust the server certificate.
    :vartype trust_server: bool
    """
    def __init__(self,
                 server_name: str,
                 database_name: str,
                 user_name: str,
                 password: Secret,
                 trust_server: bool = True):
        secret = password
        if isinstance(password, str):
            secret = register_secret(value=password, id=user_name)
        self.server = server_name
        self.credentials_type = DatabaseAuthType.SERVER
        self.database = database_name
        self.user_name = user_name
        self.password = secret
        self.trust_server = trust_server
        self.database_type = DatabaseType.MSSQL


class PostgreSQLDataSource:
    """
    Represents a datasource that points to a PostgreSQL Database.

    :var server_name: The SQL Server name.
    :vartype server: str
    :var database_name: The database name.
    :vartype database: str
    :var user_name: The username used for logging into the database.
    :vartype user_name: str
    :var password: The password used for logging into the database.
    :vartype password: str
    :var port: The port number used for connecting to the PostgreSQL server. Defaults to 5432.
    :vartype port: str
    :var ssl_mode: Indicates SSL requirement of PostgreSQL server. Defaults to Prefer.
    :vartype ssl_mode: str
    """
    def __init__(self,
                 server_name: str,
                 database_name: str,
                 user_name: str,
                 password: Secret,
                 port: str = "5432",
                 ssl_mode: DatabaseSslMode = DatabaseSslMode.PREFER):
        secret = password
        if isinstance(password, str):
            secret = register_secret(value=password, id=user_name)
        self.credentials_type = DatabaseAuthType.SERVER
        self.server = server_name
        self.database = database_name
        self.user_name = user_name
        self.password = secret
        self.database_type = DatabaseType.POSTGRESQL
        self.port_number = port
        self.ssl_mode = ssl_mode


DataSource = TypeVar('DataSource', FileDataSource, MSSQLDataSource, PostgreSQLDataSource)


class FileOutput:
    """
    Base class representing any file output target.
    """
    def __init__(self, value: OutputFilePropertyValue):
        self.underlying_value = value

    @staticmethod
    def file_output_from_str(path: str) -> 'FileOutput':
        """
        Constructs an instance of BlobFileOutput or LocalFileOutput depending on the path provided.
        """
        if blob_pattern.match(path) or wasb_pattern.match(path):
            return BlobFileOutput(path)
        else:
            return LocalFileOutput(path)


class LocalFileOutput(FileOutput):
    """
    Describes local target to write file(s) to.

    :param path: Path where output file(s) will be written to.
    """
    def __init__(self, path: Path):
        resource_details = _extract_resource_details(path, _extract_local_resource_details)
        output_file_value = OutputFilePropertyValue(target=OutputFileDestination.LOCAL,
                                                    resource_details=resource_details)
        super().__init__(output_file_value)


class BlobFileOutput(FileOutput):
    """
    Describes Azure Blob Storage target to write file(s) to.

    :param path: URL of the container where output file(s) will be written to.
    """

    def __init__(self, path: Path):
        resource_details = _extract_resource_details(path, _extract_blob_resource_details)
        output_file_value = OutputFilePropertyValue(target=OutputFileDestination.AZUREBLOB,
                                                    resource_details=resource_details)
        super().__init__(output_file_value)

class DataLakeFileOutput(FileOutput):
    """
    Describes Azure Data Lake Storage target to write file(s) to.

    :param path: URL of the container where output file(s) will be written to.
    :param access_token: (Optional) Access token.
    :param refresh_token: (Optional) Refresh token.
    :param tenant: (Optional) Tenant ID.
    """

    def __init__(self, path: Path, access_token: str = None, refresh_token: str = None, tenant: str = None):
        encoded_token = ''
        if access_token is None and refresh_token is None:
            access, refresh = get_az_cli_tokens(tenant)
            encoded_token = _DataLakeCredentialEncoder._encode_token(access, refresh)
        else:
            encoded_token = _DataLakeCredentialEncoder._encode_token(access_token, refresh_token)
        resource_details = _extract_resource_details(path, _DataLakeCredentialEncoder._register_secret(encoded_token))
        output_file_value = OutputFilePropertyValue(target=OutputFileDestination.AZUREDATALAKE,
                                                    resource_details=resource_details)
        super().__init__(output_file_value)


class ADLSGen2FileOutput(FileOutput):
    """
    Describes ADLSGen2 Storage target to write file(s) to.

    :param path: URL of the container where output file(s) will be written to.
    :param access_token: (Optional) Access token.
    :param refresh_token: (Optional) Refresh token.
    :param tenant: (Optional) Tenant ID.
    """

    def __init__(self, path: Path, access_token: str = None, refresh_token: str = None, tenant: str = None):
        encoded_token = ''
        if access_token is None and refresh_token is None:
            access, refresh = get_az_cli_tokens(tenant)
            encoded_token = _ADLSGen2CredentialEncoder._encode_token(access, refresh)
        else:
            encoded_token = _ADLSGen2CredentialEncoder._encode_token(access_token, refresh_token)
        resource_details = _extract_resource_details(path, _ADLSGen2CredentialEncoder._register_secret(encoded_token))
        output_file_value = OutputFilePropertyValue(target=OutputFileDestination.ADLSGEN2,
                                                    resource_details=resource_details)
        super().__init__(output_file_value)


DataDestination = TypeVar('DataDestination', FileOutput, 'Datastore')
