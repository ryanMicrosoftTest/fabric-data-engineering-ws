# Copyright (c) Microsoft Corporation. All rights reserved.
from .builders import InferenceArguments, FileFormatBuilder
from .dataflow import Dataflow, FilePath, DatabaseSource, SkipMode, PromoteHeadersMode
from .datasources import FileDataSource, MSSQLDataSource, PostgreSQLDataSource
from .step import Step
from ._datastore_helper import Datastore, datastore_to_dataflow, NotSupportedDatastoreTypeError
from .engineapi.typedefinitions import FileEncoding, InvalidLineHandling
from .engineapi.api import get_engine_api, EngineAPI
from ._dataframereader import get_dataframe_reader
from .parseproperties import ParseParquetProperties
from ._archiveoption import ArchiveOptions
from ._pandas_helper import have_numpy, have_pandas, ensure_df_native_compat, PandasImportError, NumpyImportError
from typing import TypeVar, List, Dict, Any, Optional
import uuid
import warnings


def _default_skip_mode(skip_mode: SkipMode, skip_rows: int) -> SkipMode:
    return SkipMode.UNGROUPED if skip_rows > 0 and skip_mode == SkipMode.NONE else skip_mode


def _handle_type_inference_and_path(df: Dataflow, inference_arguments: InferenceArguments, infer_column_types: bool, include_path: bool) -> Dataflow:
    if infer_column_types or inference_arguments is not None:
        use_inference_arguments = False
        if inference_arguments is not None:
            if infer_column_types:
                warnings.warn('inference_arguments parameter is deprecated and will be ignored.', category = DeprecationWarning, stacklevel = 2)
            else:
                use_inference_arguments = True
                warnings.warn('inference_arguments parameter is deprecated. Use infer_column_types instead.', category = DeprecationWarning, stacklevel = 2)

        column_types_builder = df.builders.set_column_types()
        if use_inference_arguments:
            column_types_builder.learn(inference_arguments)
        else:
            column_types_builder.learn()
            # no inference arguments, just drop ambiguous column conversions
            column_types_builder.ambiguous_date_conversions_drop()

        df = column_types_builder.to_dataflow()

    if not include_path:
        df = df.drop_columns(['Path'])

    return df


def read_csv(path: FilePath,
             separator: str = ',',
             header: PromoteHeadersMode = PromoteHeadersMode.CONSTANTGROUPED,
             encoding: FileEncoding = FileEncoding.UTF8,
             quoting: bool = False,
             inference_arguments: InferenceArguments = None,
             skip_rows: int = 0,
             skip_mode: SkipMode = SkipMode.NONE,
             comment: str = None,
             include_path: bool = False,
             archive_options: ArchiveOptions = None,
             infer_column_types: bool = False,
             verify_exists: bool = True,
             partition_size: Optional[int] = None,
             empty_as_string: bool = False) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read CSV and other delimited text files (TSV, custom delimiters like semicolon, colon etc.).

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param separator: The separator character to use to split columns.
    :param header: The mode in which header is promoted. The options are: `PromoteHeadersMode.CONSTANTGROUPED`, `PromoteHeadersMode.GROUPED`, `PromoteHeadersMode.NONE`, `PromoteHeadersMode.UNGROUPED`.
        The default is `PromoteHeadersMode.CONSTANTGROUPED`, which assumes all files have the same schema by promoting the first row of the first file as header, and dropping the first row of the rest of the files.
        `PromoteHeadersMode.GROUPED` will promote the first row of each file as header and aggregate the result.
        `PromoteHeadersMode.NONE` will not promote header.
        `PromoteHeadersMode.UNGROUPED` will promote only the first row of the first file as header.
    :param encoding: The encoding of the files being read.
    :param quoting: Whether to handle new line characters within quotes. The default is to interpret the new line characters as starting new rows,
        irrespective of whether the characters are within quotes or not. If set to True, new line characters inside quotes will not result in new rows, and file reading speed will slow down.
    :param inference_arguments: (Deprecated, use `infer_column_types` instead) Arguments that determine how data types are inferred.
        For example, to deal with ambiguous date format, you can specify inference_arguments = dprep.InferenceArguments(day_first = False)). Date values will then be read as MM/DD.
        Note that DataPrep will also attempt to infer and convert other column types.
    :param skip_rows: How many rows to skip in the file(s) being read.
    :param skip_mode: The mode in which rows are skipped. The options are: SkipMode.NONE, SkipMode.UNGROUPED, SkipMode.GROUPED.
        SkipMode.NONE (Default) Do not skip lines. Note that, if `skip_rows` is provided this is ignored and `SkipMode.UNGROUPED` is used instead.
        SkipMode.UNGROUPED will skip only for the first file. SkipMode.GROUPED will skip for every file.
    :param comment: Character used to indicate a line is a comment instead of data in the files being read. Comment character has to be the first character of the row to be interpreted.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param archive_options: Options for archive file, including archive type and entry glob pattern. We only support ZIP as archive type at the moment.
        For example, by specifying archive_options = ArchiveOptions(archive_type = ArchiveType.ZIP, entry_glob = '*10-20.csv'), Dataprep will read all files with name ending with "10-20.csv" in ZIP.
    :param infer_column_types: Attempt to infer columns types based on data. Apply column type conversions accordingly.
    :type infer_column_types: bool
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :param partition_size: The desired partition size in bytes. Text readers parallelize their work by splitting the
        input into partitions which can be worked on independently. This parameter makes it possible to customize the
        size of those partitions. The minimum accepted value is 4 MB (4 * 1024 * 1024).
    :param empty_as_string: Whether to keep empty field values as empty strings. Default is read them as null.
    :return: A new Dataflow.
    """
    skip_mode = _default_skip_mode(skip_mode, skip_rows)
    df = Dataflow._path_to_get_files_block(path, archive_options)
    df = df.parse_delimited(separator, header, encoding, quoting, skip_rows, skip_mode, comment, partition_size, empty_as_string)

    df = _handle_type_inference_and_path(df, inference_arguments, infer_column_types, include_path)

    if verify_exists:
        df.verify_has_data()
    return df


def read_fwf(path: FilePath,
             offsets: List[int],
             header: PromoteHeadersMode = PromoteHeadersMode.CONSTANTGROUPED,
             encoding: FileEncoding = FileEncoding.UTF8,
             inference_arguments: InferenceArguments = None,
             skip_rows: int = 0,
             skip_mode: SkipMode = SkipMode.NONE,
             include_path: bool = False,
             infer_column_types: bool = False,
             verify_exists: bool = True) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read fixed-width data.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param offsets: The offsets at which to split columns. The first column is always assumed to start at offset 0. For example, assuming we have "WAPostal98004" in a row, settling offsets = [2,8] will split the row into "WA","Postal" and "98004".
    :param header: The mode in which header is promoted. The options are: `PromoteHeadersMode.CONSTANTGROUPED`, `PromoteHeadersMode.GROUPED`, `PromoteHeadersMode.NONE`, `PromoteHeadersMode.UNGROUPED`.
        The default is `PromoteHeadersMode.CONSTANTGROUPED`, which assumes all files have the same schema by promoting the first row of the first file as header, and dropping the first row of the rest of the files.
        `PromoteHeadersMode.GROUPED` will promote the first row of each file as header and aggregate the result.
        `PromoteHeadersMode.NONE` will not promote header.
        `PromoteHeadersMode.UNGROUPED` will promote only the first row of the first file as header.
    :param encoding: The encoding of the files being read.
    :param inference_arguments: (Deprecated, use `infer_column_types` instead) Arguments that determine how data types are inferred.
        For example, to deal with ambiguous date format, you can specify inference_arguments = dprep.InferenceArguments(day_first = False)). Date values will then be read as MM/DD.
        Note that DataPrep will also attempt to infer and convert other column types.
    :param skip_rows: How many rows to skip in the file(s) being read.
    :param skip_mode: The mode in which rows are skipped. The options are: SkipMode.NONE, SkipMode.UNGROUPED, SkipMode.GROUPED.
        SkipMode.NONE (Default) Do not skip lines. Note that, if `skip_rows` is provided this is ignored and `SkipMode.UNGROUPED` is used instead.
        SkipMode.UNGROUPED will skip only for the first file. SkipMode.GROUPED will skip for every file.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param infer_column_types: Attempt to infer columns types based on data. Apply column type conversions accordingly.
    :type infer_column_types: bool
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :return: A new Dataflow.
    """
    skip_mode = _default_skip_mode(skip_mode, skip_rows)
    df = Dataflow._path_to_get_files_block(path)
    df = df.parse_fwf(offsets, header, encoding, skip_rows, skip_mode)

    df = _handle_type_inference_and_path(df, inference_arguments, infer_column_types, include_path)

    if verify_exists:
        df.verify_has_data()
    return df


def read_excel(path: FilePath,
               sheet_name: str = None,
               use_column_headers: bool = False,
               inference_arguments: InferenceArguments = None,
               skip_rows: int = 0,
               include_path: bool = False,
               infer_column_types: bool = False,
               verify_exists: bool = True) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read Excel files.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param sheet_name: The name of the Excel sheet to load.The default is to read the first sheet from each Excel file.
    :param use_column_headers: Whether to use the first row as column headers.
    :param inference_arguments: (Deprecated, use `infer_column_types` instead) Arguments that determine how data types are inferred.
        For example, to deal with ambiguous date format, you can specify inference_arguments = dprep.InferenceArguments(day_first = False)). Date values will then be read as MM/DD.
        Note that DataPrep will also attempt to infer and convert other column types.
    :param skip_rows: How many rows to skip in the file(s) being read.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param infer_column_types: Attempt to infer columns types based on data. Apply column type conversions accordingly.
    :type infer_column_types: bool
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)
    df = df.read_excel(sheet_name, use_column_headers, skip_rows)

    df = _handle_type_inference_and_path(df, inference_arguments, infer_column_types, include_path)

    if verify_exists:
        df.verify_has_data()
    return df


def read_lines(path: FilePath,
               header: PromoteHeadersMode = PromoteHeadersMode.NONE,
               encoding: FileEncoding = FileEncoding.UTF8,
               skip_rows: int = 0,
               skip_mode: SkipMode = SkipMode.NONE,
               comment: str = None,
               include_path: bool = False,
               verify_exists: bool = True,
               partition_size: Optional[int] = None) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read text files and split them into lines.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param header: The mode in which header is promoted. The options are: `PromoteHeadersMode.CONSTANTGROUPED`, `PromoteHeadersMode.GROUPED`, `PromoteHeadersMode.NONE`, `PromoteHeadersMode.UNGROUPED`.
        The default is `PromoteHeadersMode.NONE`, which will not promote header. `PromoteHeadersMode.CONSTANTGROUPED` will assume all files have the same schema by promoting the first row of the first file as header, and dropping the first row of the rest of the files.
        `PromoteHeadersMode.GROUPED` will promote the first row of each file as header and aggregate the result.
        `PromoteHeadersMode.UNGROUPED` will promote only the first row of the first file as header.
    :param encoding: The encoding of the files being read.
    :param skip_rows: How many rows to skip in the file(s) being read.
    :param skip_mode: The mode in which rows are skipped. The options are: SkipMode.NONE, SkipMode.UNGROUPED, SkipMode.GROUPED.
        SkipMode.NONE (Default) Do not skip lines. Note that, if `skip_rows` is provided this is ignored and `SkipMode.UNGROUPED` is used instead.
        SkipMode.UNGROUPED will skip only for the first file. SkipMode.GROUPED will skip for every file.
    :param comment: Character used to indicate a line is a comment instead of data in the files being read. Comment character has to be the first character of the row to be interpreted.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :param partition_size: The desired partition size in bytes. Text readers parallelize their work by splitting the
        input into partitions which can be worked on independently. This parameter makes it possible to customize the
        size of those partitions. The minimum accepted value is 4 MB (4 * 1024 * 1024).
    :return: A new Dataflow.
    """
    skip_mode = _default_skip_mode(skip_mode, skip_rows)
    df = Dataflow._path_to_get_files_block(path)
    df = df.parse_lines(header, encoding, skip_rows, skip_mode, comment, partition_size)

    if not include_path:
        df = df.drop_columns(['Path'])

    if verify_exists:
        df.verify_has_data()
    return df


def detect_file_format(path: FilePath) -> FileFormatBuilder:
    """
    Analyzes the file(s) at the specified path and attempts to determine the type of file and the arguments required
        to read it. The result is a FileFormatBuilder which contains the results of the analysis.
        This method may fail due to unsupported file format. And you should always inspect the returned builder to ensure that it is as expected.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
    :return: A FileFormatBuilder. It can be modified and used as the input to a new Dataflow
    """
    df = Dataflow._path_to_get_files_block(path)

    # File Format Detection
    ffb = df.builders.detect_file_format()
    ffb.learn()
    return ffb


def smart_read_file(path: FilePath, include_path: bool = False) -> Dataflow:
    """
    (Deprecated. Use auto_read_file instead.)

    Analyzes the file(s) at the specified path and returns a new Dataflow containing the operations required to
        read them. The type of the file and the arguments required to read it are inferred automatically.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :return: A new Dataflow.
    """
    warnings.warn('Function smart_read_file is deprecated. Use auto_read_file instead.', category = DeprecationWarning, stacklevel = 2)
    return auto_read_file(path, include_path)


def auto_read_file(path: FilePath, include_path: bool = False) -> Dataflow:
    """
    Analyzes the file(s) at the specified path and returns a new Dataflow containing the operations required to
        read them. The type of the file and the arguments required to read it are inferred automatically.
        If this method fails or produces results not as expected, you may consider using :func:`azureml.dataprep.detect_file_format` or other read methods with file types specified.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)

    # File Format Detection
    ffb = df.builders.detect_file_format()
    ffb.learn()
    df = ffb.to_dataflow(include_path = include_path)

    # Type Inference, except for parquet
    if type(ffb.file_format) != ParseParquetProperties:
        column_types_builder = df.builders.set_column_types()
        column_types_builder.learn()
        # in case any date ambiguity skip setting column type to let user address it separately.
        column_types_builder.ambiguous_date_conversions_drop()
        df = column_types_builder.to_dataflow()

    return df


def read_sql(data_source: DatabaseSource, query: str, query_timeout: int = 30) -> Dataflow:
    """
    Creates a new Dataflow that can read data from a Microsoft SQL or Azure SQL database by executing the query specified.

    :param data_source: The details of the Microsoft SQL or Azure SQL database.
    :param query: The query to execute to read data.
    :param query_timeout: Sets the wait time (in seconds) before terminating the attempt to execute a command
        and generating an error. The default is 30 seconds.
    :return: A new Dataflow.
    """
    try:
        from azureml.data.abstract_datastore import AbstractDatastore
        from azureml.data.azure_sql_database_datastore import AzureSqlDatabaseDatastore
        from azureml.data.datapath import DataPath

        if isinstance(data_source, AzureSqlDatabaseDatastore):
            return datastore_to_dataflow(DataPath(data_source, query), query_timeout)
        if isinstance(data_source, AbstractDatastore):
            raise NotSupportedDatastoreTypeError(data_source)
    except ImportError:
        pass
    df = Dataflow(get_engine_api())
    df = df.read_sql(data_source, query, query_timeout)

    return df


def read_postgresql(data_source: DatabaseSource, query: str, query_timeout: int = 20) -> Dataflow:
    """
    Creates a new Dataflow that can read data from a PostgreSQL database by executing the query specified.

    :param data_source: The details of the PostgreSQL database.
    :param query: The query to execute to read data.
    :param query_timeout: Sets the wait time (in seconds) before terminating the attempt to execute a command
        and generating an error. The default is 20 seconds.
    :return: A new Dataflow.
    """
    try:
        from azureml.data.abstract_datastore import AbstractDatastore
        from azureml.data.azure_postgre_sql_datastore import AzurePostgreSqlDatastore
        from azureml.data.datapath import DataPath

        if isinstance(data_source, AzurePostgreSqlDatastore):
            return datastore_to_dataflow(DataPath(data_source, query), query_timeout)
        if isinstance(data_source, AbstractDatastore):
            raise NotSupportedDatastoreTypeError(data_source)
    except ImportError:
        pass
    df = Dataflow(get_engine_api())
    df = df.read_postgresql(data_source, query, query_timeout)

    return df


def read_parquet_file(path: FilePath,
                      include_path: bool = False,
                      verify_exists: bool = True) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read Parquet files.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)
    df = df.read_parquet_file()

    if not include_path:
        df = df.drop_columns(['Path'])

    if verify_exists:
        df.verify_has_data()
    return df


def read_parquet_dataset(path: FilePath, include_path: bool = False) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read Parquet Datasets.

    .. remarks::

        A Parquet Dataset is different from a Parquet file in that it could be a Folder containing a number of Parquet
        Files. It could also have a hierarchical structure that partitions the data by the value of a column. These more
        complex forms of Parquet data are produced commonly by Spark/HIVE.
        read_parquet_dataset will read these more complex datasets using pyarrow which handle complex Parquet layouts
        well. It will also handle single Parquet files, or folders full of only single Parquet files, though these are
        better read using read_parquet_file as it doesn't use pyarrow for reading and should be significantly faster
        than use pyarrow.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :return: A new Dataflow.
    """
    datasource = None
    if isinstance(path, str):
        datasource = FileDataSource.datasource_from_str(path)
    elif isinstance(path, FileDataSource):
        datasource = path
    else:
        raise RuntimeError("{} is not supported by read_parquet_dataset")
    df = Dataflow.read_parquet_dataset(datasource)

    if not include_path:
        df = df.drop_columns(['Path'])

    return df


def read_preppy(path: FilePath, include_path: bool = False, verify_exists: bool = True) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read Preppy files, a file serialization format specific to Data Prep.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)
    df = df.read_preppy()

    if not include_path:
        df = df.drop_columns(['Path'])

    if verify_exists:
        df.verify_has_data()
    return df


def read_json_lines(path: FilePath,
                    encoding: FileEncoding = FileEncoding.UTF8,
                    partition_size: Optional[int] = None,
                    include_path: bool = False,
                    verify_exists: bool = True,
                    invalid_lines: InvalidLineHandling = InvalidLineHandling.ERROR) -> Dataflow:
    """
        Creates a new Dataflow with the operations required to read JSON lines files.

        :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
            Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
        :param invalid_lines: How to handle invalid JSON lines.
        :param encoding: The encoding of the files being read.
        :param partition_size: The desired partition size in bytes. Text readers parallelize their work by splitting the
            input into partitions which can be worked on independently. This parameter makes it possible to customize the
            size of those partitions. The minimum accepted value is 4 MB (4 * 1024 * 1024).
        :param include_path: Whether to include a column containing the path from which the data was read.
            This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
        :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
            to the data, but will be executed in an environment that does have access.
    """
    df = Dataflow._path_to_get_files_block(path)
    df = df.parse_json_lines(encoding=encoding, partition_size=partition_size, invalid_lines=invalid_lines)

    if not include_path:
        df = df.drop_columns(['Path'])

    if verify_exists:
        df.verify_has_data()
    return df


def read_json(path: FilePath,
              encoding: FileEncoding = FileEncoding.UTF8,
              flatten_nested_arrays: bool = False,
              include_path: bool = False) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read JSON files.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param encoding: The encoding of the files being read.
    :param flatten_nested_arrays: Property controlling program's handling of nested arrays.
        If you choose to flatten nested JSON arrays, it could result in a much larger number of rows.
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)

    # Json format detection
    builder = df.builders.extract_table_from_json()
    builder.encoding = encoding
    builder.flatten_nested_arrays = flatten_nested_arrays
    builder.learn()
    df = builder.to_dataflow()

    if not include_path:
        df = df.drop_columns(['Path'])

    return df


def read_pandas_dataframe(df: 'pandas.DataFrame',
                          temp_folder: str = None,
                          overwrite_ok: bool = True,
                          in_memory: bool = False) -> Dataflow:
    """
    Creates a new Dataflow based on the contents of a given pandas DataFrame.

    .. remarks::

        If 'in_memory' is False, the contents of 'df' will be written to 'temp_folder' as a DataPrep DataSet.
        This folder must be accessible both from the calling script and from any environment where the
        Dataflow is executed.

        If the Dataflow is guaranteed to be executed in the same context as the source DataFrame,
        the 'in_memory' argument can be set to True. In this case, the DataFrame does not need to be
        written out. This mode will usually result in better performance.

        .. note::

            The column names in the passed DataFrame must be unicode strings (or bytes). It is possible
                to end up with Integer types column names after transposing a DataFrame. These can be
                converted to strings using the command:

                .. code-block:: python

                    df.columns = df.columns.astype(str)

    :param df: pandas DataFrame to be parsed and cached at 'temp_folder'.
    :param temp_folder: path to folder that 'df' contents will be written to.
    :param overwrite_ok: If temp_folder exists, whether to allow its contents to be replaced.
    :param in_memory: Whether to read the DataFrame from memory instead of persisting to disk.
    :return: Dataflow that uses the contents of cache_path as its datasource.
    """
    if in_memory:
        return _read_in_memory_pandas_dataframe(df)

    if temp_folder is None:
        raise ValueError('temp_folder must be provided.')

    (new_schema, new_values) = ensure_df_native_compat(df)

    import os
    import shutil
    from azureml.dataprep import native

    abs_cache_path = os.path.abspath(temp_folder)
    try:
        if len(os.listdir(abs_cache_path)) > 0:
            if overwrite_ok:
                shutil.rmtree(abs_cache_path)
            else:
                raise ValueError('temp_folder must be empty.')
    except FileNotFoundError:
        pass

    os.makedirs(abs_cache_path, exist_ok = True)
    # This will write out part files to cache_path.
    native.preppy_files_from_ndarrays(new_values, new_schema, abs_cache_path)
    dflow = Dataflow.get_files(FileDataSource.datasource_from_str(os.path.join(abs_cache_path, 'part-*')))
    dflow = dflow.add_step('Microsoft.DPrep.ReadPreppyBlock', {})
    return dflow.drop_columns(['Path'])


class _DataFrameDataflow(Dataflow, object):
    def __init__(self, df: 'pandas.DataFrame', dataframe_id: str, engine_api: EngineAPI, steps: List[Step] = None):
        super().__init__(engine_api, steps)
        self._dataframe_id = dataframe_id
        self._df = df
        get_dataframe_reader().register_outgoing_dataframe(df, self._dataframe_id)

    def __del__(self):
        get_dataframe_reader().unregister_outgoing_dataframe(self._dataframe_id)

    def add_step(self,
                 step_type: str,
                 arguments: Dict[str, Any],
                 local_data: Dict[str, Any] = None) -> Dataflow:
        new_df = super().add_step(step_type, arguments, local_data)
        new_df_id = str(uuid.uuid4())
        new_df._steps[0].arguments['dataframeId'] = new_df_id
        return _DataFrameDataflow(self._df, new_df_id, self._engine_api, new_df._steps)


def _read_in_memory_pandas_dataframe(df: 'pandas.DataFrame') -> Dataflow:
    df_id = str(uuid.uuid4())
    read_df_step = Step('Microsoft.DPrep.ReadDataFrameFromSocketBlock', {
        'dataframeId': df_id
    })
    return _DataFrameDataflow(df, df_id, get_engine_api(), [read_df_step])


def read_npz_file(path: FilePath,
                  include_path: bool = False,
                  verify_exists: bool = True) -> Dataflow:
    """
    Creates a new Dataflow with the operations required to read npz files.

    :param path: The path to the file(s) or folder(s) that you want to load. It can either be a local path or an Azure Blob url.
        Globbing is supported. For example, you can use path = "./data*" to read all files with name starting with "data".
    :param include_path: Whether to include a column containing the path from which the data was read.
        This is useful when you are reading multiple files, and might want to know which file a particular record is originated from, or to keep useful information in file path.
    :param verify_exists: Checks that the file referenced exists and can be accessed by the current context. You can set this to False when creating Dataflows in an environment that does not have access
        to the data, but will be executed in an environment that does have access.
    :return: A new Dataflow.
    """
    df = Dataflow._path_to_get_files_block(path)
    df = df.read_npz_file()

    if not include_path:
        df = df.drop_columns(['Path'])

    if verify_exists:
        df.verify_has_data()
    return df
