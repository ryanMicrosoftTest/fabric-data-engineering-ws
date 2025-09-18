# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

"""
Contains functionality for interacting with existing and creating new MLTable files.

With the **mltable** package you can load, transform, and analyze data in
any Python environment, including Jupyter Notebooks or your favorite Python IDE.
"""
from .mltable import MLTable, load, from_delimited_files, from_parquet_files, from_json_lines_files, from_paths, \
    from_delta_lake, DataType, MLTableFileEncoding, MLTableHeaders


__all__ = [
    'MLTable',
    'load',
    'from_delimited_files',
    'from_parquet_files',
    'from_json_lines_files',
    'from_paths',
    'from_delta_lake',
    'DataType',
    'MLTableFileEncoding',
    'MLTableHeaders'
]

__path__ = __import__('pkgutil').extend_path(__path__, __name__)
