# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

import xml.etree.ElementTree as ET
import xmltodict
import json
from os import path, makedirs,listdir
import time
import pandas as pd
import numpy as np
import sys
import logging
import fsspec
from pprint import pprint
from pyspark.sql import Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

ingestion_metadata_dict = "{}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ingestion_metadata_dict = "{\"METADATA_CONFIG_KEY\":2,\"SOURCE_SYTEM_NAME\":\"WORKDAY\",\"DATA_FORMAT_TYPE\":\"XML\",\"API_BASE_URL\":\"https://wd2-impl-services1.workday.com/ccx/service/vcuhealth6\",\"API_NAME\":\"Get_Company_Organizations\",\"API_SERVICE_NAME\":\"Financial_Management\",\"ENVIRONMENT_NAME\":\"DEV\",\"API_VERSION\":\"v43.0\",\"bz_lakehouse\":\"WORKDAYBZ\",\"sl_lakehouse\":\"WORKDAYSL\",\"CHANGE_TRACKING\":\"0\",\"ENABLED\":true,\"PRIORITY_FLAG\":0,\"BLOB_LAST_LOAD_DT\":\"2025-09-05 11:14:19\",\"ROW_COUNT\":14}"
ingestion_metadata_dict = "{\"METADATA_CONFIG_KEY\":6,\"SOURCE_SYTEM_NAME\":\"WORKDAY\",\"DATA_FORMAT_TYPE\":\"XML\",\"API_BASE_URL\":\"https://wd2-impl-services1.workday.com/ccx/service/vcuhealth6\",\"API_NAME\":\"Get_Purchase_Items\",\"API_SERVICE_NAME\":\"Resource_Management\",\"ENVIRONMENT_NAME\":\"DEV\",\"API_VERSION\":\"v43.0\",\"bz_lakehouse\":\"WORKDAYBZ\",\"sl_lakehouse\":\"WORKDAYSL\",\"CHANGE_TRACKING\":\"0\",\"ENABLED\":true,\"PRIORITY_FLAG\":0}"
# ingestion_metadata_dict = "{\"METADATA_CONFIG_KEY\":4,\"SOURCE_SYTEM_NAME\":\"WORKDAY\",\"DATA_FORMAT_TYPE\":\"XML\",\"API_BASE_URL\":\"https://wd2-impl-services1.workday.com/ccx/service/vcuhealth6\",\"API_NAME\":\"Get_Inventory_Par_Locations\",\"API_SERVICE_NAME\":\"Inventory\",\"ENVIRONMENT_NAME\":\"DEV\",\"API_VERSION\":\"v44.1\",\"bz_lakehouse\":\"WORKDAYBZ\",\"sl_lakehouse\":\"WORKDAYSL\",\"CHANGE_TRACKING\":\"0\",\"ENABLED\":true,\"PRIORITY_FLAG\":0}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

ingestion_metadata_dict = json.loads(ingestion_metadata_dict)
print(len(ingestion_metadata_dict))
pprint(ingestion_metadata_dict)
#TODO: check the length of the dict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = time.time() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_name = 'DEV_DATAWS'
bz_lakehouse_name = ingestion_metadata_dict.get('bz_lakehouse')
sl_lakehouse_name = ingestion_metadata_dict.get('sl_lakehouse')
api_name = ingestion_metadata_dict.get('API_NAME')
api_base_url = ingestion_metadata_dict.get('API_BASE_URL')
api_service_name = ingestion_metadata_dict.get('API_SERVICE_NAME')
api_version = ingestion_metadata_dict.get('API_VERSION')
soap_endpoint = f'{api_base_url}/{api_service_name}/{api_version}/'

kv_url = 'https://vcuhseafabricdevkv001.vault.azure.net/'
print(api_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_file_path = f'abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{bz_lakehouse_name}.Lakehouse/Files'
print(base_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fsspec

AZURE_TENANT_ID = notebookutils.mssparkutils.credentials.getSecret(kv_url, 'azure-tenant-id', linkedService='')
DFS_CLIENT_ID = notebookutils.mssparkutils.credentials.getSecret(kv_url, 'spn-fabric-dev-client-id', linkedService='')
DFS_CLIENT_SECRET = notebookutils.mssparkutils.credentials.getSecret(kv_url, 'spn-fabric-dev-client-secret', linkedService='')
    
fs = fsspec.filesystem(
    "abfs"
    ,account_name="onelake"
    ,account_host= "onelake.dfs.fabric.microsoft.com"
    ,tenant_id= AZURE_TENANT_ID
    ,client_id= DFS_CLIENT_ID
    ,client_secret= DFS_CLIENT_SECRET
)

# #usage Example 1
# abfss_path = "abfss://DEV_DATAWS@onelake.dfs.fabric.microsoft.com/WORKDAYBZ.Lakehouse/Files/workday/new_config/Get_Cost_Centers.json"

# with fs.open(abfss_path,"r") as f:
#     content = f.read()
#     print(content)
# /CO_Company_Organization_Data/CO_Company_Organization_Data_20250711_152541.1.json

# #usage Example 2
# abfss_path = "abfss://DEV_DATAWS@onelake.dfs.fabric.microsoft.com/WORKDAYBZ.Lakehouse/Files/workday/new_json/Get_Company_Organizations/CO_Company_Organization_Data/CO_Company_Organization_Data_20250711_152541.1.json"
# if fs.exists(abfss_path):
#     print("exists")
# else: 
#     print("not exists")
#     with fs.open(abfss_path,"w") as f:
#         print("created")    
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

API_NAME = api_name
# API_NAME = 'Get_Cost_Centers'
# API_NAME = 'Get_Company_Organizations'
# API_NAME = 'Get_Company_ID_Definitions' 
# API_NAME = 'Get_Inventory_Par_Locations'
# API_NAME = 'Get_Purchase_Items'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ROOT_FOLDER = base_file_path+'/workday/'
# ROOT_FOLDER = '/lakehouse/default/Files/workday/'
LAST_RUN_STATS_FOLDER = ROOT_FOLDER+'last_run_stats/'+API_NAME+'/'
CONFIG_FOLDER = ROOT_FOLDER+'new_config/'
XML_RESPONSE_FOLDER = ROOT_FOLDER+'xml_response/'+API_NAME+'/'
XML_REQUEST_FOLDER = ROOT_FOLDER+'xml_requests/'+API_NAME+'/'
JSON_INGEST_FOLDER = ROOT_FOLDER+'new_json/'+API_NAME+'/'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Function {func.__name__} took {total_time:.4f} seconds")
        return result
    return wrapper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
get config info for current api from its config file
'''
@calculate_time
def read_config_file(api_name, config_folder):
    file_path = config_folder+api_name+'.json'
    print(f'Reading Config file : {file_path}')
    try:
        with fs.open(file_path, 'r') as f:
            data = json.load(f)
            print(f'Config file : {file_path} successfully read')
            return data
    except FileNotFoundError:
        # print(f"File not found: {file_path}")
        print(f'Config file : {file_path} not found')
        return None
    except json.JSONDecodeError:
        # print(f"Invalid JSON format in file: {file_path}")
        print(f'Config JSON error : {file_path}')
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_dict = read_config_file(API_NAME, CONFIG_FOLDER)
config_metadata = config_dict[0]
# SOAP_ENDPOINT = config_metadata['config_metadata'].get('api_endpoint')
SOAP_ENDPOINT = soap_endpoint
DATA_ROOT_NODE = config_metadata['config_metadata'].get('data_root_node')

KEY_CONFIG = config_metadata['config_metadata'].get('key_elements')
TOP_LEVEL_DATA_NODES = config_metadata['config_metadata'].get('top_level_data_nodes')
REF_DATA_NODES = config_metadata['config_metadata'].get('ref_data_nodes')

JSON_FILENAME_PREFIX = config_metadata['config_metadata'].get('json_file_prefix')
METADATA_NODES = config_metadata['config_metadata'].get('metadata_nodes')
REQUEST_VARS = config_metadata['config_metadata'].get('Request_Variables')

namespaces = {
     'http://schemas.xmlsoap.org/soap/envelope/': None, 
     'urn:com.workday/bsvc': None
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#process tables to be read
columns_to_parse = REF_DATA_NODES

# columns_to_parse = ['Purchase_Item_Reference',
#  'Resource_Category_Reference',
#  'Patient_Chargeable_Reference',
#  'Item_Tag_Reference',
#  'Purchase_Item_Group_Reference']

columns_to_parse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fsspec
import json

@calculate_time
def list_lakehouse_table_schemas(workspace_name: str, lakehouse_name: str, table_name: str):
    """
    Lists all tables in a Fabric Lakehouse and prints their schemas by reading Delta log files.

    Parameters:
    - workspace_name: Name of the Fabric workspace
    - lakehouse_name: Name of the Lakehouse (without .Lakehouse suffix)
    """
    # # Setup
    # filesystem_code = "abfss"
    # onelake_account_name = "onelake"
    # onelake_host = "onelake.dfs.fabric.microsoft.com"

    # # Initialize filesystem
    # fs_class = fsspec.get_filesystem_class(filesystem_code)
    # fs = fs_class(account_name=onelake_account_name, account_host=onelake_host)

    table_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{table_name}"
    # # fs = fsspec.filesystem("abfss")

    try:
        tables = fs.ls(table_path, detail=False)
    except Exception as e:
        print(f"Error accessing path: {table_path}\n{e}")
        return json.loads('{}')

    # for table_path in tables:
    # table_name = table_path.split("/")[-1]
    delta_log_path = f"{table_path}/_delta_log/"

    try:
        log_files = fs.ls(delta_log_path)
        json_logs = sorted([f for f in log_files if f.endswith(".json")])
        if not json_logs:
            print(f"No delta log found for table: {table_name}")
            return {}

        # Read the first JSON log file line by line
        with fs.open(json_logs[0], "r") as f:
            for line in f:
                entry = json.loads(line)
                if "metaData" in entry:
                    schema = entry["metaData"]["schemaString"]
                    # print(f"\n Schema for table '{table_name}':\n{schema}\n")
                    # print(entry)
                    break
        return json.loads(schema)
    except Exception as e:
        print(f"Error reading schema for table '{table_name}': {e}")



@calculate_time
def get_list_of_table_paths(workspace_name: str, lakehouse_name: str):
    # Setup
    # filesystem_code = "abfss"
    # onelake_account_name = "onelake"
    # onelake_host = "onelake.dfs.fabric.microsoft.com"

    # # Initialize filesystem
    # fs_class = fsspec.get_filesystem_class(filesystem_code)
    # fs = fs_class(account_name=onelake_account_name, account_host=onelake_host)

    target_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/"

    try:
        tables = fs.ls(target_path,detail=True)
        return tables
    except Exception as e:
        print(f"Error accessing path: {target_path}\n{e}")
        return None   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#get node from config dict that equals second arg
def get_config_dict_item(config_dict, node_key):
    node_key = node_key.strip()
    for i in range(0,len(config_dict)):
        if node_key in config_dict[i]:
            item = config_dict[i]
            # print(item)
            return item
    return {}

#get ref node xml element using node name
def get_nodes_byname(element, node_name, data_root_name = DATA_ROOT_NODE):
    ns_str = './/{*}'
    node_list = []
    node_list = element.findall(ns_str+node_name)
    if len(node_list) < 1 and node_name == data_root_name :
        node_list.append(element)
    return node_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

namespaces = {
     'http://schemas.xmlsoap.org/soap/envelope/': None, 
     'urn:com.workday/bsvc': None
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_ref_node_data(nodestr, node_name, dr_key_numeric, parent_node_key, filename, root_element_key, load_date, conf={'broadcast_conf_dict'}):
    # print(conf.values())
    ret_list = []
    str_list = []
    conf = get_config_dict_item(config_dict, node_name)
    if nodestr is None:
        return []
    nodes_str = '<node>'+nodestr+'</node>'
    root = ET.fromstring(nodes_str)
    nodes = get_nodes_byname(root, node_name)
    node_key = 0
    row_key = 0
    # print(nodeDict)
    #TODO: Check if ref nodes exist, if not, handle gracefully
    for node in nodes:

        node_key += 1
        nodeStr = ET.tostring(node)
        nodeDict = xmltodict.parse(nodeStr,
                                    process_namespaces=True,
                                    namespaces=namespaces)
        newd={}
        #for each item in the config values for that node
        for conf_val in list(conf.values())[0]:
            if isinstance(conf_val,str): #if it is a top level item in that node
                # row_key += 1
                if(conf_val in nodeDict[node_name]):
                    # print(f'{conf_val} : {nodeDict[node_name][conf_val]}')
                    newd[conf_val] = nodeDict[node_name][conf_val]
                else:
                    # print(f'{conf_val} : {None}')
                    newd[conf_val] = 'None'
            elif isinstance(conf_val,dict): #else if it is a dict
                subValueNodeName = list(conf_val.keys())[0]
                if(subValueNodeName in nodeDict[node_name]):
                    objList = []
                    if isinstance(nodeDict[node_name].get(subValueNodeName), dict):
                        objList.append(nodeDict[node_name].get(subValueNodeName))
                    else:
                        objList = nodeDict[node_name].get(subValueNodeName)

                    subdf = pd.DataFrame(objList)
                    if '@xmlns' in subdf.columns:
                        subdf = subdf.drop(columns=['@xmlns'])
                    subdf['row_key'] = row_key
                    subdf['node_key'] = node_key
                    subdf['parent_node_key'] = parent_node_key
                    subdf['root_element_key'] = root_element_key
                    subdf['dr_key_numeric'] = dr_key_numeric
                    subdf['input_file_name'] = filename
                    subdf['Load_Date'] = load_date
                    subdf = subdf.astype(str)
                    # filtered_df = subdf[subdf['root_element_key'] == 'CC100124']
                    # print(filtered_df)
                    # pivoted_df = subdf.pivot(index='node_key', columns='@type', values='#text').reset_index()
                    # other_cols = subdf.drop(columns=['@type', '#text']).drop_duplicates(subset='node_key')
                    # final_df = pd.merge(pivoted_df, other_cols, on='node_key', how='left')
                    # subdf.replace(np.nan, '',inplace=True)
                    # subdf.to_string()
                    
                    objList = subdf.to_dict(orient='records')
                    
                    for item in objList:
                        if(len(newd) > 0):
                            item.update(newd)
                        ret_list.append(item)
    # pretty_json = json.dumps(ret_list, indent=4)
    # print(pretty_json)
    return ret_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

@calculate_time
def read_delta_table(table_abfss_path):
    df = spark.read.format("delta").load(table_abfss_path)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import xml.etree.ElementTree as ET

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# source_lakehouse_name = 'WORKDAYBZ'
# source_workspace_name = 'DEV_DATAWS'
# table_name = 'STG_CC_Cost_Center_Data'


base_columns = ['root_element_key','node_key','parent_node_key', 'dr_key_numeric', 'input_file_name', 'Load_Date']

staging_table_name_prefix = f'STG_{JSON_FILENAME_PREFIX}_'
table_name_prefix = f'{JSON_FILENAME_PREFIX}_'

#TODO:need to figure out if the current context is for staging tables or other wise and 
# assign the value accordingly
target_table_name_prefix = staging_table_name_prefix



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

@calculate_time
def check_ref_column_exists(table_name:str, colums_of_interest:str):
    table_name = (staging_table_name_prefix+table_name).upper()
    table_schema = list_lakehouse_table_schemas(workspace_name, bz_lakehouse_name, table_name)
    if not table_schema :
        return []
    # print (f'Schema for table {table_name}', table_schema['fields'])
    print(f'************processing {table_name}*************')
    # get the list of columns in the current top level node table
    top_level_column_dict_list = table_schema['fields']
    top_columns = []
    for column in top_level_column_dict_list:
        top_columns.append(column['name'])

    print(f"columns in top table \n{top_columns}")
    # print(f"ref columns to parse\n{colums_of_interest}")
    ref_list = []
    # print(top_level_column_list)
    # print(json.dumps(my_dict, indent=4))
    ctr = 0

    for column in top_columns:
        # print(f"Checking column {column}")
        if(column in colums_of_interest):
            ctr= ctr+1
            ref_list.append(column)

    print(f"Total found: {ctr}")            
    print(ref_list)
    return ref_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType

@calculate_time
def create_ref_table(ref_table_name: str, source_table_name: str, column: str, columns: list, schema: dict):
    pruned_col_list = base_columns + [column]
    result_col_list = columns

    print("Creating target table:", ref_table_name)
    print("Columns in new table:", result_col_list)

    source_table_abfss_path = (
        f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{bz_lakehouse_name}.Lakehouse/Tables/{source_table_name.upper()}"
    )
    target_table_abfss_path = (
        f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{bz_lakehouse_name}.Lakehouse/Tables/{ref_table_name.upper()}"
    )
    print(f"target path {target_table_abfss_path}")

    source_df = read_delta_table(source_table_abfss_path)
    pruned_df = source_df.select([c for c in source_df.columns if c in pruned_col_list])
    # display(pruned_df)
    print(f"**************Number of partitions: {pruned_df.rdd.getNumPartitions()}********************")

    # Your existing JSON-producing UDF (returns JSON string)
    json_udf = F.udf(get_ref_node_data, ArrayType(MapType(StringType(), StringType())))

    print(f'Processing - {column}')
    with_array = pruned_df.withColumn(
        "json_column",
        json_udf(
            F.col(column), F.lit(column),
            F.col('dr_key_numeric'), F.col('node_key'),
            F.col('input_file_name'), F.col('root_element_key'),
            F.col('Load_Date')
        )
    )

    display(with_array)
    df_out = (
        with_array
        .withColumn("m", F.explode("json_column"))     # m is a MAP<STRING,STRING>
        .select(*[F.col("m").getItem(k).alias(k) for k in columns])
        )

    display(df_out)
    
    

    # If you know the keys you actually need, select them here.
    # Otherwise, keep as map in a single column, or use Option 2b to discover keys on a sample.
    # Example: keep as map for now
    
    # df_final = exploded.select(
    #     *[F.col(c) for c in result_col_list],
    #     F.col("json_item")
    # )
    # display(df_final)

    # Control output files without shuffle
    target_files = 64
    (df_out.coalesce(target_files)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_table_abfss_path)
    )
    print(f"Table created at {target_table_abfss_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

def _dedupe_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def _collect_fields(struct, include_dict_keys=False, extra_fields=None):
    """
    Extracts fields from a structure like: [str, {key: [str, str, ...]}]
    - include_dict_keys=True will include the dict keys (e.g., 'ID') as columns.
    - extra_fields: list[str] to append to the collected fields.

    Returns: list[str] (unique, order-preserving)
    """
    fields = []
    for col in struct:
        if isinstance(col, str):
            fields.append(col)
        elif isinstance(col, dict):
            # print(list(item.values()))
            vals = [item for sublist in col.values() for item in sublist]
            # subItem = list(item.values())
            fields.extend(vals)
    
    

    # if isinstance(struct, list) and struct:
    #     # 1) First item if it's a string
    #     first = struct[0]
    #     print(f"first: {first}")
    #     if isinstance(first, str):
    #         fields.append(first)

    #     # 2) Second item
    #     if len(struct) > 1:
    #         second = struct[1]
    #         if isinstance(second, dict):
    #             if include_dict_keys:
    #                 fields.extend([k for k in second.keys() if isinstance(k, str)])
    #             for v in second.values():
    #                 if isinstance(v, list):
    #                     fields.extend([item for item in v if isinstance(item, str)])
    #                 elif isinstance(v, str):
    #                     fields.append(v)
    #         elif isinstance(second, list):
    #             fields.extend([item for item in second if isinstance(item, str)])

    # # 3) Add caller-specified fields
    if extra_fields:
        fields.extend([f for f in extra_fields if isinstance(f, str)])
    print(fields)
    return _dedupe_preserve_order(fields)

def string_schema_from_structure(struct, *, include_dict_keys=False, extra_fields=None):
    """
    Build a StructType with StringType fields from the structure,
    optionally including dict keys and extra fields.
    """
    cols = _collect_fields(struct, include_dict_keys=include_dict_keys, extra_fields=extra_fields)
    return StructType([StructField(c, StringType(), True) for c in cols])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REF_DAG = {
    "activities":[],
    "timeoutInSeconds": 43200, # max timeout for the entire DAG, default to 12 hours
    "concurrency": 20 # max number of notebooks to run concurrently, defaults to 50 but ultimately constrained by the number of driver cores
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# target_ref_table_name = f'{target_table_name_prefix}{table}_{column_name}'
            
print(TOP_LEVEL_DATA_NODES)
#for every top level table loop through and do the following:
# check_ref_column_exists(TOP_LEVEL_DATA_NODES[0], columns_to_parse)
for table in TOP_LEVEL_DATA_NODES:
    
    source_table_name = staging_table_name_prefix + table
    print(f'Parsing Table: {source_table_name}')
    ref_list = check_ref_column_exists(table, columns_to_parse)
    
    # print('****',ref_list)
    if len(ref_list) > 0:
        for ref_col in ref_list:
            act = {}
            args = {}
            inputs = {}
            ref_table_name = staging_table_name_prefix+table+'_'+ref_col
            act["name"] = ref_table_name # activity name, must be unique
            act["path"] = "nb_1300_wkdy_parse_ref_dag_child"
            act["timeoutPerCellInSeconds"]= 3600 # max timeout for each cell, default to 90 seconds
            
            print(f'Creating table: {ref_table_name} from {source_table_name}')
            conf = get_config_dict_item(config_dict, ref_col)
            # pprint(conf)
            structure = list(conf.values())[0]
            # pprint(f"STRUCTURE: {structure}")
            cols = _collect_fields(structure, include_dict_keys=False, extra_fields=base_columns)
            # print(cols)
            # print(f"Columns in {ref_table_name} {cols}:")
            # -> ['@Descriptor', 'ID', '@type', '#text', 'created_at', 'source']
            schema = string_schema_from_structure(structure, include_dict_keys=False, extra_fields=base_columns)
            schema_dict = {f.name: f.dataType.simpleString() for f in schema.fields}
            args["ref_table_name"] = ref_table_name
            args["source_table_name"] = source_table_name
            args["ref_col"] = ref_col
            args["cols"] = cols
            args["schema_dict"] = schema_dict
            args["bz_lakehouse_name"] = bz_lakehouse_name
            args["workspace_name"] = workspace_name
            args["base_columns"]  = base_columns
            args["config_dict"]  = config_dict
            inputs["inputs"] = json.dumps(args)
            act["args"] = inputs
            REF_DAG["activities"].append(act)
    else:
        print(f'Top Level Table {source_table_name} not found!')
print(f"Activity DAG :")
pprint(REF_DAG)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.runMultiple(REF_DAG, {"displayDAGViaGraphviz": False})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = time.time()
total_time_seconds = end_time - start_time
total_time = round(total_time_seconds/60,2)
print(f'Completed Processing all tables in {total_time} minutes')
print('test')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
