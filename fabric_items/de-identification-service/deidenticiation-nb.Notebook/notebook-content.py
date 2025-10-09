# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Welcome to your new notebook
# # Type here in the cell editor to add code!


# CELL ********************

%pip install azure-health-deidentification --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install dotenv --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install urllib3 --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install pydicom --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.health.deidentification import DeidentificationClient
from azure.health.deidentification.models import DeidentificationJob, SourceStorageLocation, TargetStorageLocation, DeidentificationContent, DeidentificationOperationType
from azure.identity import DefaultAzureCredential
from azure.core.polling import LROPoller
import os
from dotenv import load_dotenv
from azure.core.exceptions import HttpResponseError
from notebookutils.credentials import getSecret
import logging
import sys
from urllib3.filepost import encode_multipart_formdata, choose_boundary
import requests
from pathlib import Path
import pydicom
import uuid
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.dataframe
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from azure.storage.blob import BlobClient


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show azure.health.deidentification

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_adls(df:pyspark.sql.dataframe, container:str, storage_account:str, path:str, type:str='parquet')-> None:
    """
    Utility to write to an ADLS path via abfss

    df: DataFrame: The dataframe to write to ADLS
    container:str: The name of the container to write to
    storage_account:str The name of the storage account
    path:str:  The path to persist the data to

    returns: None
    """
    target = f'abfss://{container}@{storage_account}.dfs.core.windows.net/{path}/'
    print(target)
    df.write.format(type).mode('overwrite').save(target)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Fabric_Health_Utils:

    @staticmethod
    def initialize_logs(log_handle:str, log_level: int=logging.DEBUG):
        """
        Pass in handler to initialize logger
        Set log levels to DEBUG if no log lever passed in
        """
        # set logging basics to logging not set
        logging.basicConfig(level=log_level)
    

        logger = logging.getLogger(log_handle)
        logger.setLevel(log_level)  # Set the level for the specific logger
        if not logger.handlers:
            ch = logging.StreamHandler(sys.stdout)  # Ensure the handler outputs to stdout
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p'
            )
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DeidentifyService:

    def __init__(self, log_level=logging.WARN):
        os.environ["AZURE_CLIENT_ID"] = '76badb38-46cb-463a-a44e-a29c93346d4e'
        os.environ["AZURE_TENANT_ID"] = '35acf02c-4b87-4ae6-9221-ff5cafd430b4'
        key_vault = 'https://healthcarefabrickv.vault.azure.net/'
        secret_name = 'wsid-client-secret'                                    # this is the secret name of the workspace identity

        secret = getSecret(key_vault, secret_name)
        os.environ["AZURE_CLIENT_SECRET"] = secret

        deid_ep = "https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com"

        self.client = DeidentificationClient(endpoint=deid_ep, credential=DefaultAzureCredential())
        self.logger = Fabric_Health_Utils.initialize_logs(log_handle='DeidentifyService', log_level=log_level)
        self.logger.info('DeidentifyService initialized')

    def call_deidentify_service(self,operation, text):
        """
        Call the DeIdentification Service

        operation: REDACT, SURROGATE, TAG
        text: text to be deidentified
        """
        data = {
            "inputText": text,
            "operation": operation      # REDACT, SURROGATE, TAG
        }

        self.logger.info(f'My Data Input is:\n {data}')
        try:
            result = self.client.deidentify(data)
        except HttpResponseError as e:
            self.logger.error(f'INFO: service responds error: {e.response.json()}')
        
        return result


    def call_deidentify_service_new(self,operation, text):
        """
        Call the DeIdentification Service

        operation: REDACT, SURROGATE, TAG
        text: text to be deidentified
        """
        body = DeidentificationContent(
        input_text="Hello, I'm Dr. John Smith.", operation_type=DeidentificationOperationType.TAG
            )

        result: DeidentificationResult = self.client.deidentify_text(body)
        print(f'\nOriginal Text:    "{body.input_text}"')

        if result.tagger_result and result.tagger_result.entities:
            print(f"Tagged Entities:")
            for entity in result.tagger_result.entities:
                print(
                    f'\tEntity Text: "{entity.text}", Entity Category: "{entity.category}", Offset: "{entity.offset.code_point}", Length: "{entity.length.code_point}"'
                )
        else:
            print("\tNo tagged entities found.")
            
        return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DICOM:
    def __init__(self, log_level=logging.DEBUG):
        """
        Uses existing resources to create a client used to communicate with the DICOM service
        """
        self.logger = Fabric_Health_Utils.initialize_logs(log_handle='DICOM', log_level=log_level)

        self.logger.info('Initializing DICOM service')

        self.logger.info('Setting abfss path')
        self.abfs_path = 'abfss://c0a7b8a9-eb12-495a-b863-2cb583e31154@onelake.dfs.fabric.microsoft.com/025ef395-cb42-4882-987d-04785949b475/Files'

        self.logger.info('Setting base url')
        self.base_url = "https://azhealthaidsrheus2-dicomservicesrheus2.dicom.azurehealthcareapis.com/v2" 

        # Set the service principal credentials 
        os.environ['AZURE_CLIENT_ID'] = '4c4c5ec4-e892-4609-a6f8-0261f76a28b0'
        key_vault = 'https://healthcarefabrickv.vault.azure.net/'
        secret_name = 'wsid-client-secret'

        self.logger.info('Setting secret')
        secret = getSecret(key_vault, secret_name)
        os.environ["AZURE_CLIENT_SECRET"] = secret
        os.environ['AZURE_TENANT_ID'] = '35acf02c-4b87-4ae6-9221-ff5cafd430b4' 


        self.logger.info('Setting lakehouse path')
        lakehouse_path = f'{self.abfs_path}/dicom_data' 
        credential = DefaultAzureCredential()


        # Get an access token for the DICOM service 
        self.logger.info('Getting access token from dicom service') 
        self.token = credential.get_token("https://dicom.healthcareapis.azure.com/.default")
        

        self.logger.info('Setting client')
        self.client = requests.Session()

        # verify authentication is configured correctly
        headers = {"Authorization":self.token.token}
        url= f'{self.base_url}/changefeed'
        
        try:
            response = self.client.get(url,headers=headers)
        except Exception as e:
            self.logger.error(f'Error! Likely not authenticated!: {e}')
        

        self.logger.info('DICOM initialized')

    def encode_multipart_related(fields, boundary=None):
        """
        The request library and most python libraries don't work with multipart\related in a way that supports
        DICOMweb.  Because of these libraries, we must add a few methods to support working with DICOM files

        encode_multipart_related takes a set of fields (in the DICOM case, these libraries are generally Part 10 dam files) and an optional
        user-defined boundary.

        It returns both the full body, along with the content_type, which can be used
        """
        if boundary is None:
            boundary = choose_boundary()

        body, _ = encode_multipart_formdata(fields, boundary)
        content_type = str('multipart/related; boundary=%s' %boundary)

        return body, content_type

    def upload_dicom(self, file_path, storage_account, container_name):
        """
        Upload DICOM file to Azure Blob Storage
        """
        self.logger.info(f'Uploading {file_path} to {storage_account}/{container_name}')
        
        url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{Path(file_path).name}"
        
        with open(file_path, 'rb') as f:
            files = {'file': (Path(file_path).name, f)}
            boundary = choose_boundary(files)
            body, content_type = encode_multipart_formdata(files, boundary)
            headers = {
                'Content-Type': content_type,
                'x-ms-blob-type': 'BlockBlob'
            }
            response = requests.put(url, headers=headers, data=body)
        
        if response.status_code == 201:
            self.logger.info(f'Successfully uploaded {file_path}')
        else:
            self.logger.error(f'Failed to upload {file_path}: {response.text}')

    def download_dicom(self, storage_account, container_name, blob_name, download_path):
        """
        Download DICOM file from Azure Blob Storage
        """
        self.logger.info(f'Downloading {blob_name} from {storage_account}/{container_name} to {download_path}')
        
        url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{blob_name}"
        
        response = requests.get(url)
        
        if response.status_code == 200:
            with open(download_path, 'wb') as f:
                f.write(response.content)
            self.logger.info(f'Successfully downloaded {blob_name}')
        else:
            self.logger.error(f'Failed to download {blob_name}: {response.text}')

        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Client Code - Realtime De-Identification

# CELL ********************

# setup deidentification client
# Recording --> Transcription --> Output

client = DeidentifyService()

# CALCULATE THREE SAMPLES
data_sample = 'My name is Dr. John patient Smith and my phone number is 555-1234 with patient ID 23534634634 with SSN: 456-22-1234 and Medical Record Number: 4567986056'

# Vistit Details: [Doctor] saw [patient] with [SSN] on [date] and observed xxx and prescribed 


result = client.call_deidentify_service_new('REDACT', data_sample)

"""
print('REDACT')
result = client.call_deidentify_service('REDACT', data_sample)
display(result)

print('SURROGATE')
result = client.call_deidentify_service('SURROGATE', data_sample)
display(result)

print('TAG')
result = client.call_deidentify_service('TAG', data_sample)
display(result)

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Client Code - Batch Deidentification Jobs - Storage Account with .txt files

# CELL ********************

# sa:     name: healthsaadlsrheus
# https://<storageaccount>.blob.core.windows.net/<container>

endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes'
outputPrefix = "doctor-notes"

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Client Code - Parquet Files in Storage Account and the output written to delta tables in Fabric

# CELL ********************

# grab from lakehouse shortcut
path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Files/raw-health-data/doctor-notes/*.txt'
rdd = spark.sparkContext.wholeTextFiles(path)

# convert to dataframe with column
doctor_notes_df = rdd.toDF(['file_path', 'content'])

# ensure one partition
doctor_notes_df = doctor_notes_df.coalesce(1)

# add another column with a monotonically increasing id
doctor_notes_df = doctor_notes_df.withColumn('id', monotonically_increasing_id())

display(doctor_notes_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write to storage account as parquet
target_container = 'raw-health-data'
target_storage_account = 'healthsaadlsrheus'
target_path = 'doctor-notes-parquet'

write_to_adls(doctor_notes_df, target_container, target_storage_account, target_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### read from shortcut to parquet adls path where files just dropped --> Not working

""" COMMENTING OUT SECTION AS THIS ISN'T WORKING

# read from storage account parquet input and run de-identification job
endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes-parquet'
outputPrefix = "doctor-notes-parquet"

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-parquet-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')



job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)


output_read_path = f'abfss://{container_name_output}@{sa_name}.dfs.core.windows.net/{outputPrefix}/*.parquet'



# read from storage account and write to lakehouse
deidentified_parquet_df = spark.read.format('parquet').load(output_read_path)

display(deidentified_parquet_df)

""" # COMMENTING OUT SECTION AS THIS ISN'T WORKING

# sending parquet files to the de-identification service appears to have issues
# new assumption.  Source data cannot be compressed. CANT FIND SOURCE TO VALIDATE THIS, ONLY WHAT HAS BEEN EXPERIENCED

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Modified Attempt, write just a single table column as parquet
# However, how can we tie this back to the original data without having to do a fuzzy lookup?

# CELL ********************

# grab from lakehouse shortcut
path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Files/raw-health-data/doctor-notes/*.txt'
rdd = spark.sparkContext.wholeTextFiles(path)

# convert to dataframe with column
doctor_notes_single_col_df = rdd.toDF(['file_path', 'content'])

# drop file path col
doctor_notes_single_col_df = doctor_notes_single_col_df.drop('file_path')

display(doctor_notes_single_col_df)

# write to storage account as parquet
target_container = 'raw-health-data'
target_storage_account = 'healthsaadlsrheus'
target_path = 'doctor-notes-parquet-single-column'

write_to_adls(doctor_notes_df, target_container, target_storage_account, target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read from storage account parquet input and run de-identification job

""" COMMENTING OUT SECTION AS THIS ISN'T WORKING

endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes-parquet-single-column'
outputPrefix = "doctor-notes-parquet-single-column"

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-parquet-col-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')



job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)


output_read_path = f'abfss://{container_name_output}@{sa_name}.dfs.core.windows.net/{outputPrefix}/*.parquet'


# read from storage account and write to lakehouse
deidentified_parquet_df = spark.read.format('parquet').load(output_read_path)

display(deidentified_parquet_df)

""" # COMMENTING OUT SECTION AS THIS ISN'T WORKING

# same error.  Perhaps try as csv and see if we get the same result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Attempt with CSV Table
# Works as expected

# CELL ********************

# grab from lakehouse shortcut
path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Files/raw-health-data/doctor-notes/*.txt'
rdd = spark.sparkContext.wholeTextFiles(path)

# convert to dataframe with column
doctor_notes_csv_df = rdd.toDF(['file_path', 'content'])

# ensure one partition
doctor_notes_csv_df = doctor_notes_csv_df.coalesce(1)

# add another column with a monotonically increasing id
doctor_notes_csv_df = doctor_notes_csv_df.withColumn('id', monotonically_increasing_id())

display(doctor_notes_csv_df)

# write to storage account as parquet
target_container = 'raw-health-data'
target_storage_account = 'healthsaadlsrheus'
target_path = 'doctor-notes-csv-table'

write_to_adls(doctor_notes_csv_df, target_container, target_storage_account, target_path, type='csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read from storage account parquet input and run de-identification job

endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes-csv-table'
outputPrefix = 'doctor-notes-csv-table-2'

# Output read path is: abfss://deidentified-data@healthsaadlsrheus.dfs.core.windows.net/doctor-notes-csv-table/*.csv

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-csv-table-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')



job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read input
# https://healthsaadlsrheus.blob.core.windows.net/raw-health-data/doctor-notes-csv-table/part-00000-8689e5b8-34c3-4e45-9fd4-d943efa33f80-c000.csv
input_read_path = f'abfss://{container_name_input}@{sa_name}.dfs.core.windows.net/{inputPrefix}/*.csv'

print(f'Input read path is: {input_read_path}')

input_csv_df = spark.read.format('csv').option('header', 'false').option('multiline', 'true').load(input_read_path)

# rename columns
input_csv_df = (input_csv_df
                        .withColumnRenamed('_c0', 'file_path')
                        .withColumnRenamed('_c1', 'content')
                        .withColumnRenamed('_c2', 'id')
)

display(input_csv_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output_read_path = f'abfss://{container_name_output}@{sa_name}.dfs.core.windows.net/{outputPrefix}/*.csv'

print(f'Output read path is: {output_read_path}')

deidentified_csv_df = spark.read.format('csv').option('header', 'false').option('multiline', 'true').load(output_read_path)

# rename columns
deidentified_csv_df = (deidentified_csv_df
                        .withColumnRenamed('_c0', 'file_path')
                        .withColumnRenamed('_c1', 'content')
                        .withColumnRenamed('_c2', 'id')
)

display(deidentified_csv_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rdd = spark.sparkContext.wholeTextFiles(output_read_path)

# convert to dataframe with column
deidentified_csv_df = rdd.toDF(['file_path', 'content'])



display(deidentified_csv_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Recommended Approach
# Land raw data in ADLS (just the data to be de-identified) <br>
# Run deidentification service <br>
# land deidentified data in deidentified container as .txt files.  This is because the service expects unstructured text, not structured or binary formats as input<br>
# fabric shortcut on deidentified csv data <br>
# convert to delta table in fabric <br>
# 
# - file path is used to link identified file to de-identified file

# CELL ********************

# sa:     name: healthsaadlsrheus
# https://<storageaccount>.blob.core.windows.net/<container>

endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes'
outputPrefix = "doctor-notes-final"

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read data from de-identified data shortcut
rdd = spark.sparkContext.wholeTextFiles('abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Files/doctor-notes-final/*.txt')

deid_data_df = rdd.toDF(['file_path', 'memo_content'])

# keep to 1 partition
deid_data_df = deid_data_df.coalesce(1)

# add monotonically increasing id column
deid_data_df = deid_data_df.withColumn('id', monotonically_increasing_id())

display(deid_data_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write to lakehouse table
target_lakehouse_table_path = 'abfss://a8cbda3d-903e-4154-97d9-9a91c95abb42@onelake.dfs.fabric.microsoft.com/5090acaa-9246-4eb2-b9fc-93b8b6e7e60c/Tables/deidentified_dr_notes'

deid_data_df.write.format('delta').mode('overwrite').save(target_lakehouse_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Example if the data is coming from a SQL Database
# Extract the column <br>
# Persist the column as unstructured text in the storage account <br>
# Run deidentification service <br>
# combine with data

# CELL ********************

import pyodbc

class SQLutils:
    @staticmethod
    def connect_sql_database(server_name:str, database_name:str, username:str, password:str):
        """
        Make a pyodbc connection to azure sql database and return a client object for other functions to leverage

        parameters
        server_name:str: The name of the SQL Server to connect to
        database_name:str: The name of the database to connect to
        username:str: The username to be used as a client for the connection
        password:str: The password for the username used in the connection

        returns:
            conn
        """
        driver = '{ODBC Driver 18 for SQL Server}'

        conn_str = f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()

        return conn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get secrets from key vault
username = notebookutils.credentials.getSecret('https://healthcarefabrickv.vault.azure.net/', 'healthdb-username')
password = notebookutils.credentials.getSecret('https://healthcarefabrickv.vault.azure.net/', 'healthdb-password')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# connect to azure sql health database
# Driver={ODBC Driver 18 for SQL Server};Server=tcp:fabricorchestrationserver.database.windows.net,1433;Database=healthDB;Uid=rharrington;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;

sql_conn = SQLutils.connect_sql_database(server_name='fabricorchestrationserver.database.windows.net', database_name='healthDB', username=username, password=password)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cursor = sql_conn.cursor()

cursor.execute('SELECT * FROM DoctorMemos')

rows = cursor.fetchall()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# convert to easier to digest object for dataframe

columns = [col[0] for col in cursor.description]

# convert each row to a dict
data = [dict(zip(columns,row)) for row in rows]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('doctor_name', StringType(), True),
        StructField('memo_date', DateType(), True),
        StructField('memo', StringType(), True)
    ]
)

sql_dr_memos_df = spark.createDataFrame(data, schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sql_dr_memos_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# extract just the memo column (or column to be deidentified) and save to storage account as text
deidentified_column_df = sql_dr_memos_df.select('memo')

display(deidentified_column_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# split into individual files
deid_list = [r[0] for r in deidentified_column_df.collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write each file to azure storage
sa_name = 'healthsaadlsrheus'
container_name = 'raw-health-data'
target_prefix = 'doctor-notes-sql'

# blob endpoint
blob_ep = f'https://{sa_name}.blob.core.windows.net'

# set credentials for the workspace identity
os.environ["AZURE_CLIENT_ID"] = '76badb38-46cb-463a-a44e-a29c93346d4e'
os.environ["AZURE_TENANT_ID"] = '35acf02c-4b87-4ae6-9221-ff5cafd430b4'
key_vault = 'https://healthcarefabrickv.vault.azure.net/'
secret_name = 'wsid-client-secret'                                    # this is the secret name of the workspace identity

secret = getSecret(key_vault, secret_name)
os.environ["AZURE_CLIENT_SECRET"] = secret

credential = DefaultAzureCredential()


for idx, memo in enumerate(deid_list):
    print(idx)
    print(memo)
    blob_name = f'{target_prefix}/memo_{idx}.txt'
    blob_client = BlobClient(
        account_url=blob_ep,
        container_name= container_name,
        blob_name = blob_name,
        credential = credential
        )

    # upload as utf-8 text
    blob_client.upload_blob(memo.encode('utf-8'), overwrite=True, content_type='text/plain')

    print(f'uploaded: {blob_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# now the deidentification service can be called like in the examples above with a DeidentificationClient and DeidentificationJob
# sa:     name: healthsaadlsrheus
# https://<storageaccount>.blob.core.windows.net/<container>

endpoint = 'https://hkhmf3dreqa5fahv.api.eus2001.deid.azure.com'
sa_name = 'healthsaadlsrheus'
container_name_input = 'raw-health-data'
container_name_output = 'deidentified-data'
storage_location_input = f'https://{sa_name}.blob.core.windows.net/{container_name_input}'
storage_location_output = f'https://{sa_name}.blob.core.windows.net/{container_name_output}'
inputPrefix = 'doctor-notes-sql'
outputPrefix = "doctor-notes-sql"

credential = DefaultAzureCredential()

client = DeidentificationClient(endpoint, credential)

jobname = f"doctor-notes-{uuid.uuid4().hex[:8]}"

print(f'Name of the job is: {jobname}')



job = DeidentificationJob(
    source_location=SourceStorageLocation(
        location=storage_location_input,
        prefix=inputPrefix,
    ),
    target_location=TargetStorageLocation(location=storage_location_output, prefix=outputPrefix, overwrite=True),
)

finished_job: DeidentificationJob = client.begin_deidentify_documents(jobname, job).result(timeout=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
