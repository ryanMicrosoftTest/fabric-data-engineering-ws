"""
Contains functionality for running common data preparation tasks in Azure Machine Learning.

With the **dataprep** package you can load, transform, analyze, and write data in machine learning workflows in
any Python environment, including Jupyter Notebooks or your favorite Python IDE.

This package is internal, and is not intended to be used directly.
"""
# Copyright (c) Microsoft Corporation. All rights reserved.
# Here many parts of the DataPrep API are re-exported under the 'azureml.dataprep' namespace for convenience.

# extend_path searches all directories on sys.path for any packages that exists under the same namespace
# as __name__. This handles different modules from the same namespace being installed in different places.
# https://docs.python.org/3.6/library/pkgutil.html#pkgutil.extend_path

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

# Builders
from .api.builders import InferenceArguments, FileFormatArguments, SourceData, ImputeColumnArguments
from .api.engineapi.typedefinitions import ReplaceValueFunction, StringMissingReplacementOption

# Dataflow
from .api.dataflow import Dataflow, DataflowReference, HistogramArgumentsValue, \
    KernelDensityArgumentsValue, SummaryColumnsValue, SummaryFunction, JoinType, SType

# Engine Types used by Dataflow
from .api.engineapi.typedefinitions import ColumnRelationship, DecimalMark, TrimType, \
    MismatchAsOption, AssertPolicy

# DataProfile
from .api.dataprofile import ColumnProfile, DataProfile, HistogramBucket, ValueCountEntry, TypeCountEntry, STypeCountEntry

# DataSources
from .api.datasources import Path, LocalDataSource, BlobDataSource, DatabaseAuthType, MSSQLDataSource, PostgreSQLDataSource, \
    LocalFileOutput, BlobFileOutput, HttpDataSource, DatabaseSslMode

# Inspectors
from .api.inspector import *

# Expressions
from .api.expressions import col, f_not, f_and, f_or, cond, Expression, ExpressionLike, value

# Functions
from .api.functions import round, trim_string, get_stream_name, RegEx, create_datetime, get_stream_properties, get_stream_info, \
    create_http_stream_info

# Parse Properties
from .api.parseproperties import ParseDelimitedProperties, ParseFixedWidthProperties, ParseLinesProperties, \
    ParseParquetProperties, ReadExcelProperties, ReadJsonProperties, ParseDatasourceProperties

# Readers
from .api.readers import FilePath, read_csv, read_fwf, read_excel, read_lines, read_sql, read_postgresql, read_parquet_file, \
    read_parquet_dataset, read_json, detect_file_format, smart_read_file, auto_read_file, read_pandas_dataframe, read_npz_file, \
    SkipMode, PromoteHeadersMode, FileEncoding, read_json_lines, read_preppy, InvalidLineHandling
from .api._archiveoption import ArchiveOptions
from .api.engineapi.typedefinitions import ArchiveType

# References
from .api.references import ExternalReference

# Rslex Executor
from .api._rslex_executor import use_rust_execution

# Secret Manager
from .api.secretmanager import Secret, register_secrets, register_secret, create_secret

# Steps
from .api.step import ColumnSelector, MultiColumnSelection, Step

# Type Conversions
from .api.typeconversions import FieldType, TypeConverter, FloatConverter, DateTimeConverter, \
    CandidateDateTimeConverter, CandidateConverter, InferenceInfo, StreamInfoConverter

# Types
from .api.types import SplitExample, Delimiters

# AML
from .api._datastore_helper import login

# Error handling
from .api.errorhandlers import ExecutionError, UnexpectedError, ValidationError, DataPrepException

# Spark execution
from .api.sparkexecution import DataPrepImportError, set_execution_mode

# Profile Diff related objects
from .api.engineapi.typedefinitions import (HistogramCompareMethod, ColumnProfileDifference, DataProfileDifference)

from .api._loggerfactory import set_diagnostics_collection

# MLTable Helper
from .api.mltable._mltable_helper import _download_mltable_yaml, _read_yaml, UserErrorException

# Expose types for documentation
__all__ = ['Dataflow', 'read_csv', 'read_fwf', 'read_excel', 'read_lines', 'read_sql', 'read_postgresql', 'read_parquet_file',
           'read_parquet_dataset', 'read_json', 'detect_file_format', 'auto_read_file', 'read_pandas_dataframe', 'use_rust_execution',
           'InferenceArguments', 'FilePath', 'SkipMode', 'PromoteHeadersMode', 'FileEncoding', 'JoinType',
           'FileFormatArguments', 'DataflowReference', 'SourceData', 'ImputeColumnArguments', 'ColumnSelector',
           'MultiColumnSelection', 'Path', 'LocalDataSource', 'BlobDataSource', 'DatabaseAuthType', 'MSSQLDataSource', 'PostgreSQLDataSource',
           'LocalFileOutput', 'BlobFileOutput', 'HttpDataSource', 'ReplaceValueFunction', 'StringMissingReplacementOption', 'login',
           'HistogramArgumentsValue', 'KernelDensityArgumentsValue', 'SummaryColumnsValue',
           'ColumnRelationship', 'DecimalMark', 'SummaryFunction', 'TrimType', 'MismatchAsOption', 'AssertPolicy',
           'TypeConverter', 'FieldType', 'FloatConverter', 'DateTimeConverter', 'CandidateDateTimeConverter', 'CandidateConverter',
           'StreamInfoConverter', 'InferenceInfo', 'ColumnProfile', 'DataProfile', 'HistogramBucket', 'ValueCountEntry', 'TypeCountEntry',
           'ParseDelimitedProperties', 'ParseFixedWidthProperties', 'ParseLinesProperties', 'ParseParquetProperties',
           'ReadExcelProperties', 'ReadJsonProperties', 'ParseDatasourceProperties', 'ExternalReference', 'col', 'f_not', 'f_and', 'f_or',
           'cond', 'Expression', 'ExpressionLike', 'value', 'round', 'trim_string', 'Secret', 'register_secrets',
           'register_secret', 'create_secret', 'SplitExample', 'Delimiters',
           'DataPrepException', 'ExecutionError', 'ValidationError', 'UnexpectedError', 'DataPrepImportError', 'Step', 'SType',
           'STypeCountEntry', 'HistogramCompareMethod', 'set_diagnostics_collection', 'RegEx', 'create_datetime', 'get_stream_properties',
           'get_stream_info', 'read_preppy', 'create_http_stream_info', 'DatabaseSslMode', '_read_yaml']


__version__ = '4.10.6'
