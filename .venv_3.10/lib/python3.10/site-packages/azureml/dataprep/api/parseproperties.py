# Copyright (c) Microsoft Corporation. All rights reserved.
from .engineapi.typedefinitions import PromoteHeadersMode, SkipMode, FileEncoding, DataSourceProperties
from typing import TypeVar, List, cast
from textwrap import dedent


def _generate_standard_props(datasource_properties: DataSourceProperties):
    return {
        'header': PromoteHeadersMode.CONSTANTGROUPED if datasource_properties.promote_headers else PromoteHeadersMode.NONE,
        'encoding': FileEncoding(datasource_properties.encoding),
        'skip_rows': cast(int, datasource_properties.skip_lines_count),
        'skip_mode': SkipMode.NONE if datasource_properties.skip_lines_count == 0 else SkipMode.GROUPED
    }


def parse_properties_from_datasource_properties(datasource_properties: DataSourceProperties):
    if not datasource_properties or not datasource_properties.data_source_type:
        raise NotImplementedError("Detected file type not supported by DataPrep API")

    if datasource_properties.data_source_type == 'Microsoft.DPrep.ParseDelimitedBlock':
        return ParseDelimitedProperties(
            separator=datasource_properties.delimiter,
            **_generate_standard_props(datasource_properties)
        )
    if datasource_properties.data_source_type == 'Microsoft.DPrep.ParseFixedWidthColumns':
        return ParseFixedWidthProperties(
            offsets=cast(List[int], datasource_properties.column_positions),
            **_generate_standard_props(datasource_properties)
        )
    if datasource_properties.data_source_type == 'Microsoft.DPrep.ParsePlainTextBlock':
        return ParseLinesProperties(**_generate_standard_props(datasource_properties))
    if datasource_properties.data_source_type == 'Microsoft.DPrep.ReadExcelBlock':
        return ReadExcelProperties(
            use_column_headers=datasource_properties.promote_headers,
            skip_rows=cast(int, datasource_properties.skip_lines_count)
        )
    if datasource_properties.data_source_type == 'Microsoft.DPrep.ReadParquetFileBlock':
        return ParseParquetProperties()
    if datasource_properties.data_source_type == 'JSONFile':
        return ReadJsonProperties(encoding=FileEncoding(datasource_properties.encoding), json_extract_program='')
    raise NotImplementedError("Detected file type not supported by DataPrep API")


class ParseDelimitedProperties:
    """
    Describes and stores the properties required to parse a Delimited Text-file.
    """
    def __init__(self,
                 separator: str = ',',
                 header: PromoteHeadersMode = PromoteHeadersMode.CONSTANTGROUPED,
                 encoding: FileEncoding = FileEncoding.UTF8,
                 quoting: bool = False,
                 skip_rows: int = 0,
                 skip_mode: SkipMode = SkipMode.NONE,
                 comment: str = None):
        self.separator = separator
        self.headers_mode = header
        self.encoding = encoding
        self.quoting = quoting
        self.skip_rows = skip_rows
        self.skip_mode = skip_mode
        self.comment = comment

    def __repr__(self):
        return dedent("""\
        ParseDelimitedProperties
            separator: '{separator}'
            headers_mode: {headers_mode}
            encoding: {encoding}
            quoting: {quoting}
            skip_rows: {skip_rows}
            skip_mode: {skip_mode}
            comment: {comment}
        """.format(**vars(self)))


class ParseFixedWidthProperties:
    """
    Describes and stores the properties required to parse a Fixed-Width Text-file.
    """
    def __init__(self,
                 offsets: List[int],
                 header: PromoteHeadersMode = PromoteHeadersMode.CONSTANTGROUPED,
                 encoding: FileEncoding = FileEncoding.UTF8,
                 skip_rows: int = 0,
                 skip_mode: SkipMode = SkipMode.NONE):
        self.offsets = offsets
        self.headers_mode = header
        self.encoding = encoding
        self.skip_rows = skip_rows
        self.skip_mode = skip_mode

    def __repr__(self):
        return dedent("""\
        ParseFixedWidthProperties
            offsets: '{offsets}'
            headers_mode: {headers_mode}
            encoding: {encoding}
            skip_rows: {skip_rows}
            skip_mode: {skip_mode}
        """.format(**vars(self)))


class ParseLinesProperties:
    """
    Describes and stores the properties required to parse a Text-file containing raw lines.
    """
    def __init__(self,
                 header: PromoteHeadersMode = PromoteHeadersMode.CONSTANTGROUPED,
                 encoding: FileEncoding = FileEncoding.UTF8,
                 skip_rows: int = 0,
                 skip_mode: SkipMode = SkipMode.NONE,
                 comment: str = None):
        self.headers_mode = header
        self.encoding = encoding
        self.skip_rows = skip_rows
        self.skip_mode = skip_mode
        self.comment = comment

    def __repr__(self):
        return dedent("""\
        ParseLinesProperties
            headers_mode: {headers_mode}
            encoding: {encoding}
            skip_rows: {skip_rows}
            skip_mode: {skip_mode}
            comment: {comment}
        """.format(**vars(self)))


class ReadExcelProperties:
    """
    Describes and stores the properties required to read an Excel file.
    """
    def __init__(self,
                 sheet_name: str = None,
                 use_column_headers: bool = False,
                 skip_rows: int = 0):
        self.sheet_name = sheet_name
        self.use_column_headers = use_column_headers
        self.skip_rows = skip_rows

    def __repr__(self):
        return dedent("""\
        ReadExcelProperties
            sheet_name: {sheet_name}
            use_column_headers: {use_column_headers}
            skip_rows: {skip_rows}
        """.format(**vars(self)))


class ReadJsonProperties:
    """
    Describes and stores the properties required to read a JSON file.
    """
    def __init__(self,
                 json_extract_program: str = '',
                 encoding: FileEncoding = FileEncoding.UTF8):
        self.json_extract_program = json_extract_program
        self.encoding = encoding

    def __repr__(self):
        return dedent("""\
        ReadJsonProperties
            json_extract_program: {json_extract_program}
            encoding: {encoding}
        """.format(**vars(self)))


class ParseParquetProperties:
    """
    Describes and stores the properties required to read a Parquet File.
    """
    def __init__(self):
        pass

    def __repr__(self):
        return dedent("""\
        ParseParquetProperties
        """).format(**vars(self))


ParseDatasourceProperties = TypeVar('ParseDatasourceProperties',
                                    ParseDelimitedProperties,
                                    ParseFixedWidthProperties,
                                    ParseLinesProperties,
                                    ParseParquetProperties,
                                    ReadExcelProperties,
                                    ReadJsonProperties)
