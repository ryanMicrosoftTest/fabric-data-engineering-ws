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

# CELL ********************

import json

dag = {
    'activities': [
        {
            'name': 'unique_activity_name',
            'path': 'api-ingestion-to-files-bronze',
            'timeoutPerCellInSeconds': 90,
            'args': {
                'kv_name': 'kvfabricprodeus2rh',
                'secret_name': 'fh-api-secret',
                'base_url': 'https://finnhub.io/api/v1/',
                'api': 'stock/insider-sentiment',
                'kv_args': """{ "symbol": "TSLA", "from": "2015-01-01", "to": "2022-03-01" }"""
            }

        }
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    results = mssparkutils.notebook.runMultiple(dag)
except Exception as e:
    results = getattr(e, 'result', None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

type(kv_args)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import json
from urllib.parse import urlencode

# Your kv_args is currently a JSON string; parse it first
kv_args_str = """
{ "symbol": "TSLA", "from": "2015-01-01", "to": "2022-03-01" }
"""
kv_args_dict = json.loads(kv_args_str)

# Encode as query string
encoded_args = urlencode(kv_args_dict)
print(encoded_args)
print(type(encoded_args))
# Output: symbol=TSLA&from=2015-01-01&to=2022-03-01


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# debug section below
kv_name = 'kvfabricprodeus2rh'
secret_name = 'fh-api-secret'
base_url = 'https://finnhub.io/api/v1/'
api = 'stock/insider-sentiment'
kv_args = """
{ "symbol": "TSLA", "from": "2015-01-01", "to": "2022-03-01" }
"""


from fabric_utils.api_utils import ApiUtils

# make api request


secret_value = notebookutils.credentials.getSecret(f'https://{kv_name}.vault.azure.net/', secret_name)

headers = {
    "X-Finnhub-Token": secret_value
}


resp_tuple = ApiUtils.api_request_with_stats(base_url=base_url, api=api, headers=headers, url_args=kv_args)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

resp_tuple

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
