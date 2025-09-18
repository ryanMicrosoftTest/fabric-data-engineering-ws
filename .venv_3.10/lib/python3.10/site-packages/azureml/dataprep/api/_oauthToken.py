# Copyright (c) Microsoft Corporation. All rights reserved.
import json
import os
import os.path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from ._constants import AZURE_CLIENT_ID

ResourceMgmtUrl = 'https://management.core.windows.net'
DatalakeResourceUrl = 'https://datalake.azure.net/'
AzureOAuthTokenEndpoint = 'https://login.microsoftonline.com/{0}/oauth2/token'

def _read_tokens(tokenJsonFile: str) -> object:
    if not os.path.isfile(tokenJsonFile):
        return []
    with open(tokenJsonFile, 'rt', encoding='utf-8') as jsonFile:
        jsonContent = jsonFile.read()
        parsedTokens = None
        try:
            parsedTokens = json.loads(jsonContent)
        finally:
            if parsedTokens is None:
                return []
        return parsedTokens

def get_az_cli_tokens(tenant: str) -> (str, str):
    azCliTokenFile = os.path.join(_get_config_dir(), 'accessTokens.json')
    tokens = _read_tokens(azCliTokenFile)
    if tokens is not None and len(tokens) > 0:
        datalakeToken = next((entry for entry in tokens if entry.get('resource', '').startswith(DatalakeResourceUrl)), None)
        if datalakeToken is not None:
            return datalakeToken['accessToken'], datalakeToken['refreshToken']
        else:
            mgmtToken = next((entry for entry in tokens if entry.get('resource', '').startswith(ResourceMgmtUrl) and entry.get('isMRRT', False)), None)
            if mgmtToken is not None:
                tokenResp = _get_token_with_refresh_token(mgmtToken['refreshToken'], DatalakeResourceUrl, tenant)
                return tokenResp['access_token'], tokenResp['refresh_token']
    raise RuntimeError("Cannot find Azure CLI's OAuth tokens cache file, please login in with 'az login' first!")

def _get_config_dir() -> str:
    return os.getenv('AZURE_CONFIG_DIR') or os.path.expanduser(os.path.join('~', '.azure'))

# https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-protocols-oauth-code
def _get_token_with_refresh_token(refresh_token:str, resource: str, tenant: str) -> object:
    oauth_params = {
        'grant_type': 'refresh_token',
        'client_id': AZURE_CLIENT_ID,
        'refresh_token': refresh_token,
        'resource': resource
    }
    encoded = urlencode(oauth_params)
    endpoint = AzureOAuthTokenEndpoint.format(tenant or 'common')
    request = Request(endpoint, bytearray(encoded, 'utf-8'), {
        'content-type':'application/x-www-form-urlencoded',
        'accept-charset': 'utf-8'
    }, method='POST')
    resp = {}
    try:
        resp = urlopen(request)
    except HTTPError as ex:
        errMsg = ex.read()
        raise RuntimeError('http request to {0}: {1} {2} ({3})'.format(request.get_full_url(), ex.code, ex.msg, errMsg))
    if (resp.code != 200 and 'application/json' not in resp.headers['Content-Type']):
        raise RuntimeError("Cannot refresh access token for resource {0}, error: {1}".format(resource, resp.get_code()))
    tokenResp = json.loads(resp.read())
    return tokenResp
