# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
import json
from jsonschema import validate
import os
import re
import yaml

from azureml.dataprep.api.mltable._mltable_helper import _parse_path_format, _PathType
from azureml.dataprep.api._loggerfactory import _LoggerFactory


_logger = _LoggerFactory.get_logger('MLTableUtils')
_long_form_aml_uri = re.compile(
    r'^azureml://subscriptions/([^\/]+)/resourcegroups/([^\/]+)/'
    r'(?:providers/Microsoft.MachineLearningServices/)?workspaces/([^\/]+)/(.*)',
    re.IGNORECASE)


_PATHS_KEY = 'paths'


def _is_local_path(path):
    return _parse_path_format(path)[0] == _PathType.local


def _make_all_paths_absolute(mltable_yaml_dict, base_path):
    def format_path_pair(path_prop, path, is_base_path_local):
        # get absolute path from base_path + relative path
        if _is_local_path(path) and not path.startswith('file://'):
            # assumes that local relative paths are co-located in directory of MLTable file
            path = os.path.normpath(path)

            if not os.path.isabs(path):
                # when path == '.' it represents the current dir, which is base_path ex) folder: .
                path = base_path if _path_is_current_directory_variant(path) else os.path.join(base_path, path)

            if is_base_path_local:
                path = os.path.normpath(path)
                path = "file://" + os.path.abspath(path)

        return path_prop, path

    def format_paths(paths, is_base_path_local):
        return list(map(
            lambda path_dict: dict(map(lambda x: format_path_pair(*x, is_base_path_local), path_dict.items())), paths))

    if base_path and _PATHS_KEY in mltable_yaml_dict:
        mltable_yaml_dict[_PATHS_KEY] = format_paths(mltable_yaml_dict[_PATHS_KEY], _is_local_path(base_path))
    return mltable_yaml_dict


def _path_is_current_directory_variant(path):
    return path in ['.', './', '.\\']


def _validate(mltable_yaml_dict):
    cwd = os.path.dirname(os.path.abspath(__file__))
    schema_path = "{}/schema/MLTable.json".format(cwd.rstrip("/"))
    with open(schema_path, "r") as stream:
        try:
            schema = json.load(stream)
        except json.decoder.JSONDecodeError:
            raise RuntimeError("MLTable json schema is not a valid json file.")
    try:
        validate(mltable_yaml_dict, schema)
    except Exception as e:
        _logger.warning(f"MLTable validation failed with error: {e.args[0]}")
        raise ValueError(f"Given MLTable does not adhere to the AzureML MLTable schema: {e.args[0]}")


# will switch to the api from dataprep package once new dataprep version is released
def _parse_workspace_context_from_longform_uri(uri):
    long_form_uri_match = _long_form_aml_uri.match(uri)

    if long_form_uri_match:
        return {
            'subscription': long_form_uri_match.group(1),
            'resource_group': long_form_uri_match.group(2),
            'workspace_name': long_form_uri_match.group(3)
        }

    return None


# utility function to remove all the empty and null fields from a nested dict
def _remove_empty_and_null_fields(mltable_yaml_dict):
    if isinstance(mltable_yaml_dict, dict):
        return {k : v for k, v in
                ((k, _remove_empty_and_null_fields(v)) for k, v in mltable_yaml_dict.items()) if v is not None}
    if isinstance(mltable_yaml_dict, list):
        return [v for v in map(_remove_empty_and_null_fields, mltable_yaml_dict) if v is not None]
    return mltable_yaml_dict


class MLTableYamlCleaner(yaml.YAMLObject):
    # _dataflow.to_yaml_string() serializes Serde units (anonymous value containing no data) as nulls.
    # this results in nested fields with empty values being serialized with nulls as values.
    def __init__(self, mltable_yaml_dict):
        self.cleaned_mltable_yaml_dict = _remove_empty_and_null_fields(mltable_yaml_dict)

    def __repr__(self):
        return yaml.dump(self.cleaned_mltable_yaml_dict)
