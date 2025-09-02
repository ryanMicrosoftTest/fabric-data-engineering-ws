from azure.ai.ml import MLClient
from azure.ai.ml.entities import Data
from azure.identity import DefaultAzureCredential

ml_client = MLClient(
    DefaultAzureCredential(),
    subscription_id="910ebf13-1058-405d-b6cf-eda03e5288d1",
    resource_group_name="aml-rg",
    workspace_name="aml-private-dev"
)

data_asset = Data(
    name = 'employee_mltable',
    version = '1',
    type = 'mltable',
    path='azureml://datastores/adls_datastore/paths/employee/'
)

ml_client.data.create_or_update(data_asset)