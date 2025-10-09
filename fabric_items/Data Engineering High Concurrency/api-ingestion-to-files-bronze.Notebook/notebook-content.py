# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "5310979b-49b7-82b4-4033-b21c0e4be517",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

kv_name = 'kvfabricprodeus2rh'
secret_name = 'fh-api-secret'
base_url = 'https://finnhub.io/api/v1/'
api = 'stock/insider-sentiment'
kv_args = """
{ "symbol": "TSLA", "from": "2015-01-01", "to": "2022-03-01" }
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from urllib.parse import urlencode

kv_args_str = kv_args
kv_args_dict = json.loads(kv_args_str)

# Encode as query string
encoded_args = urlencode(kv_args_dict)
print(encoded_args)
print(type(encoded_args))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from fabric_utils.api_utils import ApiUtils

help(ApiUtils.api_request_with_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# make api request


secret_value = notebookutils.credentials.getSecret(f'https://{kv_name}.vault.azure.net/', secret_name)

headers = {
    "X-Finnhub-Token": secret_value
}


resp_tuple = ApiUtils.api_request_with_stats(base_url=base_url, api=api, headers=headers, url_args=encoded_args)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(resp_tuple)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp_tuple[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(resp_tuple)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
