from azure.ai.ml import MLClient, command, Input
from azure.ai.ml.constants import AssetTypes, InputOutputModes
from azure.identity import DefaultAzureCredential
# from azureml.data import FileDataset, TabularDataset
import mltable
import pandas as pd

ml_client = MLClient.from_config(credential=DefaultAzureCredential())
data_asset = ml_client.data.get("employee_uri_folder", version="1")
print(data_asset)

# datastore_name = "eablob001_mlcontainer_dfs_los_data_folder"

data_path = [{"pattern":data_asset.path}]

tbl = mltable.from_parquet_files(paths=data_path)
df = tbl.to_pandas_dataframe()

# df = pd.read_parquet(data_asset.path)

print(df.head())



python -m pip install azure-ai-ml pandas pyarrow fsspec adlfs