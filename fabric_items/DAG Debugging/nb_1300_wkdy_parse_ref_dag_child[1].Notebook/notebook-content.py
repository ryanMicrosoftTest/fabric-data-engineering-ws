# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
inputs = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

inputs = json.loads(inputs)

cols = inputs["cols"]
ref_col = inputs["ref_col"]
ref_table_name = inputs["ref_table_name"]
schema_dict = inputs["schema_dict"]
source_table_name = inputs["source_table_name"]
bz_lakehouse_name = inputs["bz_lakehouse_name"]
workspace_name = inputs["workspace_name"]
base_columns = inputs["base_columns"]
config_dict = inputs["config_dict"]
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

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import xml.etree.ElementTree as ET
from pyspark.sql.functions import col, lit, from_json, explode, size
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, MapType
import xmltodict
import pandas as pd
import numpy as np


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
def get_nodes_byname(element, node_name, data_root_name = "DATA_ROOT_NODE"):
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_delta_table(table_abfss_path):
    df = spark.read.format("delta").load(table_abfss_path)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

    df_out = (
        with_array
        .withColumn("m", F.explode("json_column"))     # m is a MAP<STRING,STRING>
        .select(*[F.col("m").getItem(k).alias(k) for k in columns])
        )

    display(df_out)
    
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

create_ref_table(ref_table_name, source_table_name, ref_col, cols, schema_dict)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
