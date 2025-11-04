# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "98142750-133b-48db-a038-ffec3f8427df",
# META       "default_lakehouse_name": "presidio_lh",
# META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
# META       "known_lakehouses": [
# META         {
# META           "id": "98142750-133b-48db-a038-ffec3f8427df"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "5ae7d1d1-e806-bbae-440e-994359ed87b6",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%pip install numpy==1.26.3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install /lakehouse/default/Files/presidio/models/en_core_web_lg-3.8.0-py3-none-any.whl  
# Installing the large model from the lakehouse as it exceeds the size limit for custom libraries in the Fabric environment.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    array, lit, explode, col, monotonically_increasing_id, concat
)
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import pandas_udf, PandasUDFType
from presidio_anonymizer.entities import OperatorConfig
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

num_duplicates = 5000 # for the scale part
csv_path = "Files/presidio/data/sample_users_500.csv"
is_write_to_delta = True
table_namne = "presidio_demo_table"
partitions_number = 100

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

configuration = {
    "nlp_engine_name": "spacy",
    "models": [
        {"lang_code": "en", "model_name": "en_core_web_lg"},
    ]
}

provider = NlpEngineProvider(nlp_configuration=configuration)
nlp_engine = provider.create_engine()

# create analyzer engine to identify PII in text
analyzer = AnalyzerEngine(
    nlp_engine=nlp_engine, supported_languages=["en"]
)


text_to_analyze = "His name is Mr. Jones and his phone number is 212-555-5555"
analyzer_results = analyzer.analyze(text=text_to_analyze, entities=["PHONE_NUMBER"], language='en')

print(analyzer_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create anonymizer to de-identify redacted PII entities
# This is broadcasted to all nodes to avoid initializing the large spacy model on each node (and instead broadcasting out a copy to each node)

anonymizer = AnonymizerEngine()
broadcasted_analyzer = spark.sparkContext.broadcast(analyzer)
broadcasted_anonymizer = spark.sparkContext.broadcast(anonymizer)
df = spark.read.format("csv").option("header", "true").load(csv_path)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def detect_pii_in_row(*cols):
    """
    Analyze each column separately so we know which substring (entity text) 
    belongs to which column. Return a dict {col_name: [ 'ENTITY_TYPE: substring', ... ] }.
    add this column to the dataframe
    """
    analyzer = broadcasted_analyzer.value
    col_names = detect_pii_in_row.col_names
    entities_found = {}

    for idx, val in enumerate(cols):
        if val is None:
            continue
        column_text = str(val)
        print(f'INFO: Analyzing column: {val}')
        results = analyzer.analyze(text=column_text, language="en")

        if results:
            # Example: ["PERSON: John Doe", "PHONE_NUMBER: 212-555-1111", ...]
            found_entities = []
            for res in results:
                substring = column_text[res.start:res.end]  # The actual text recognized
                entity_str = f"{res.entity_type}: {substring}"
                print(f'INFO:  Discovered entity string: {entity_str}')
                found_entities.append(entity_str)
            
            entities_found[col_names[idx]] = found_entities

    # If no PII was detected at all
    if not entities_found:
        return "No PII"
    return str(entities_found)

detect_pii_in_row.col_names = df.columns
detect_pii_udf = udf(detect_pii_in_row, StringType())

df_with_pii_summary = df.withColumn(
    "pii_summary",
    detect_pii_udf(*[col(c) for c in df.columns])
)

display(df_with_pii_summary)
# df_with_pii_summary.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def anonymize_text(text: str) -> str:
    """
    Detect PII in the given text using the large model and replace it with an empty string.
    """
    if text is None:
        return None

    # get the executor local copiees of the broadcast variables so the udf can use them without re-creating the presidio engines for each task
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value


    analyze_results = analyzer.analyze(text=text, language="en")

    # text is the string you want anonymized; analyzer results is a list of detected PII entities; operators is a dict that tells Presidio how to transform each detected entity type
    # DEFAULT means apply to value to all entity types; OperatorConfig("replace", {"new_value": ""}) means replace the detected PII with an empty string
    anonymized_result = anonymizer.anonymize(
        text=text,
        analyzer_results=analyze_results,
        operators={"DEFAULT": OperatorConfig("replace", {"new_value": ""})}
)
    return anonymized_result.text

# Registering as a regular PySpark UDF
anon_udf = udf(anonymize_text, StringType())

df_final = df_with_pii_summary.withColumn("anon_user_query", anon_udf(col("user_query")))

display(df_final)
# df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_expanded = df.withColumn(
    "duplication_array",
    array([lit(i) for i in range(num_duplicates)])
)

df_test = df_expanded.withColumn("duplicate_id", explode(col("duplication_array")))

df_test = df_test.withColumn("id", monotonically_increasing_id())

df_test = df_test.withColumn(
    "user_query",
    concat(col("user_query"), lit(" - ID: "), col("id"))
)
df_test = df_test.drop("duplication_array", "duplicate_id")
df_test = df_test.repartition(partitions_number) # repartition to show parrallel processing -should be remove/modify to allow high scales.
df_test = df_test.withColumn("anon_user_query", anon_udf(col("user_query")))
print(f"total row number {df_test.count()}") # Number of duplicates X number of rows in the DF
display(df_test.limit(partitions_number))
# (df_test.limit(partitions_number)).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

broadcasted_analyzer.value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_test.write.format('delta').save('Files/presidio/data/users_output.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Presidio Structured

# CELL ********************

# load sample data
url = 'https://raw.githubusercontent.com/rkniyer999/pii-sparkshield/refs/heads/main/data/customer-profile-sample-data/customers-sampledata.csv'

customer_pd_df = pd.read_csv('https://raw.githubusercontent.com/rkniyer999/pii-sparkshield/refs/heads/main/data/customer-profile-sample-data/customers-sampledata.csv')

customer_spark_df = spark.createDataFrame(customer_pd_df)

display(customer_spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# add a columns name attribute to the function for the function to reference it internally
detect_pii_in_row.col_names = customer_spark_df.columns


customer_df_with_pii_summary = customer_spark_df.withColumn(
    "pii_summary",
    detect_pii_udf(*[col(c) for c in customer_spark_df.columns])
)

display(customer_df_with_pii_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df_with_pii_summary_final = customer_df_with_pii_summary.withColumn("anon_user_query", anon_udf(col("Email")))

display(customer_df_with_pii_summary_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load in doctor_memos

doctor_memos_df = spark.read.format('delta').load('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/doctor_memos_fact_obt')

display(doctor_memos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

detect_pii_in_row.col_names = doctor_memos_df.columns


doctor_df_with_pii_summary = doctor_memos_df.withColumn(
    "pii_summary",
    detect_pii_udf(*[col(c) for c in doctor_memos_df.columns])
)

display(doctor_df_with_pii_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def anonymize_text_options(text: str, entity_type_list:list) -> str:
    """
    Detect PII in the given text using the large model and replace it with an empty string.

    args:
        text:str:  The text to be evaludated for anonymization
        entity_type_list:list: What detected PII to handle
    """
    if text is None:
        return None

    # get the executor local copiees of the broadcast variables so the udf can use them without re-creating the presidio engines for each task
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value


    analyze_results = analyzer.analyze(text=text, language="en", entities = entity_type_list)

    # text is the string you want anonymized; analyzer results is a list of detected PII entities; operators is a dict that tells Presidio how to transform each detected entity type
    # DEFAULT means apply to value to all entity types; OperatorConfig("replace", {"new_value": ""}) means replace the detected PII with an empty string
    anonymized_result = anonymizer.anonymize(
        text=text,
        analyzer_results=analyze_results,
        operators={"DEFAULT": OperatorConfig("replace", {"new_value": ""})}
)
    return anonymized_result.text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

anon_options_udf = udf(anonymize_text_options, StringType())
anon_entities_list = ["PERSON", "EMAIL_ADDRESS"]

doctor_df_with_pii_summary_final = doctor_df_with_pii_summary.withColumn("anonymized_memo_content", 
    anon_options_udf(col("memo_content"), 
    F.array(*[F.lit(x) for x in anon_entities_list])
    )
)

display(doctor_df_with_pii_summary_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Anonymization in a format that can be deanonymized

# CELL ********************

import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get a secret key from Key Vault.  This will be used as the encryption key
KEY = mssparkutils.credentials.getSecret('https://kvfabricprodeus2rh.vault.azure.net/', 'PRESIDIO-ENCRYPTION-KEY')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

aes_key = base64.b64decode(KEY.strip())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

configuration = {
    "nlp_engine_name": "spacy",
    "models": [
        {"lang_code": "en", "model_name": "en_core_web_lg"},
    ]
}

provider = NlpEngineProvider(nlp_configuration=configuration)
nlp_engine = provider.create_engine()

# create analyzer engine to identify PII in text
analyzer = AnalyzerEngine(
    nlp_engine=nlp_engine, supported_languages=["en"]
)


text = 'Ryan emailed me at ryan@company.org'
analyze_results = analyzer.analyze(text=text, entities=["PERSON", "EMAIL_ADDRESS"], language='en')

print(analyzer_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# configure an anonymizer
anonymizer = AnonymizerEngine()

ops = {
    'PERSON': OperatorConfig('encrypt', {'key':aes_key}),
    'EMAIL_ADDRESS': OperatorConfig('encrypt', {'key':aes_key})
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# quick test
text = 'Ryan emailed me at ryan@company.org'
res = anonymizer.anonymize(text=text, analyzer_results=analyze_results, operators=ops)

print(res.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Deanonymize data back to the original text
# Note, you can only deanonymize what was anonymized with a reversible operator.  replace, as in: [operators={"DEFAULT": OperatorConfig("replace", {"new_value": ""})}] is not reversible
# To support deanonymization, you must: 1. Anonymize with a reversible operator like encrypt 2. Persist the anonymization metadata returned by Presidio and 3 later call DeanonymizationEngine.deanonymize() with the same key and the saved spans
from presidio_anonymizer import DeanonymizeEngine

# Initialize the engine:
engine = DeanonymizeEngine()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# build entities
from presidio_anonymizer.entities import OperatorResult


enc_items_json = json.dumps([
    {"start": it.start, "end": it.end, "entity_type": it.entity_type}
    for it in res.items
])


entities = [
    OperatorResult(start=e["start"], end=e["end"], entity_type=e["entity_type"])
    for e in json.loads(enc_items_json)
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

entities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

res.text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(ops_decrypt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ops_decrypt = {
    'PERSON':        OperatorConfig('decrypt', {'key': aes_key}),
    'EMAIL_ADDRESS': OperatorConfig('decrypt', {'key': aes_key})
}


decrypted_res = engine.deanonymize(
            text=res.text,
            entities=[
                OperatorResult(start=59, end=123, entity_type='EMAIL_ADDRESS'),
                OperatorResult(start=0, end=44, entity_type='PERSON')
            ],
            operators=ops_decrypt
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

decrypted_res.text

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
