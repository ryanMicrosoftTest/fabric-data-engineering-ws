# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

"""Contains helper methods for mltable apis."""
import re
import yaml
import os

from enum import Enum

from azureml.dataprep.api._loggerfactory import _LoggerFactory

logger = _LoggerFactory.get_logger("MLTableHelper")

class UserErrorException(Exception):
    """Exception to separate user errors from system errors"""
    pass


def _download_mltable_yaml(path: str):
    from azureml.dataprep.rslex import Copier, PyLocationInfo, PyIfDestinationExists
    from azureml.dataprep.api._rslex_executor import ensure_rslex_environment

    ensure_rslex_environment()

    # normalize path to MLTable yaml path
    normalized_path = "{}/MLTable".format(path.rstrip("/"))
    if_destination_exists = PyIfDestinationExists.MERGE_WITH_OVERWRITE
    try:

        from tempfile import mkdtemp
        local_path = mkdtemp()
        Copier.copy_uri(PyLocationInfo('Local', local_path, {}),
                        normalized_path, if_destination_exists, "")

        return local_path
    except Exception as e:
        if "InvalidUriScheme" in e.args[0] or \
                "DataAccessError(NotFound)" in e.args[0]:
            raise UserErrorException(e.args[0])
        elif "StreamError(NotFound)" in e.args[0]:
            raise UserErrorException(e.args[0] + "; Not able to find MLTable file")
        elif "ExecutionError(StreamError(PermissionDenied" in e.args[0]:
            raise UserErrorException(f'Getting permission error when trying to access MLTable,'
                             f'please make sure proper access is configured on storage: {e.args[0]}')
        elif "No identity was found on compute" in e.args[0]:
            raise UserErrorException(e.args[0])
        else:
            raise SystemError(e.args[0])


def _is_tabular(mltable_yaml):
    if mltable_yaml is None:
        return False

    transformations_key = "transformations"
    if transformations_key not in mltable_yaml.keys():
        return False

    tabular_transformations = [
        "read_delimited",
        "read_parquet",
        "read_json_lines"
    ]

    if mltable_yaml['transformations'] and all(isinstance(e, dict) for e in mltable_yaml['transformations']):
        list_of_transformations = [k for d in mltable_yaml['transformations'] for k, v in d.items()]
    else:
        # case where transformations section is a list[str], not a list[dict].
        list_of_transformations = mltable_yaml['transformations']
    return any(key in tabular_transformations for key in list_of_transformations)


def _read_yaml(uri):
    local_yaml_path = "{}/MLTable".format(uri.rstrip("/"))
    if not os.path.exists(local_yaml_path):
        raise UserErrorException('Not able to find MLTable file from the MLTable folder')

    mltable_yaml_dict = None
    with open(local_yaml_path, "r") as stream:
        try:
            mltable_yaml_dict = yaml.safe_load(stream)
        except yaml.YAMLError:
            raise UserErrorException('MLTable yaml is invalid: {}'.format(mltable_yaml_dict))
    return mltable_yaml_dict


def _path_has_parent_directory_redirection(path):
    return path.startswith('..') or '/..' in path or '\\..' in path


def _make_all_paths_absolute(mltable_yaml_dict, local_path):
    if 'paths' in mltable_yaml_dict:
        for path_dict in mltable_yaml_dict['paths']:
            for path_prop, path in path_dict.items():
                path_type, _, _ = _parse_path_format(path)
                if path_type == _PathType.local and not os.path.isabs(path):
                    if _path_has_parent_directory_redirection(path):
                        logger.info("Path has parent directory redirection. path: {}".format(path))
                    path_dict[path_prop] = os.path.normpath(os.path.join(local_path, path))
                    if os.path.commonprefix([path_dict[path_prop], local_path]) != local_path:
                        raise UserErrorException("Path is outside the mltable directory. path: {}".format(path))
                    path_dict[path_prop] = "file://" + path_dict[path_prop]
    return mltable_yaml_dict


class _PathType(Enum):
    local = 1
    cloud = 2
    legacy_dataset = 3
    data_asset_uri = 4


def _parse_path_format(path: str, workspace=None):
    data_asset_uri_pattern = re.compile(
        r'^azureml://subscriptions/([^\/]+)/resourcegroups/([^\/]+)/'
        r'(?:providers/Microsoft.MachineLearningServices/)?workspaces/([^\/]+)/data/([^\/]+)/versions/(.*)',
        re.IGNORECASE)
    data_asset_uri_match = data_asset_uri_pattern.match(path)
    if data_asset_uri_match:
        return _PathType.data_asset_uri, path, \
               (data_asset_uri_match.group(1), data_asset_uri_match.group(2), data_asset_uri_match.group(3),
                data_asset_uri_match.group(4), data_asset_uri_match.group(5))

    regular_cloud_uri_patterns = \
        re.compile(r'^https?://|adl://|wasbs?://|abfss?://|azureml://subscriptions', re.IGNORECASE)
    if regular_cloud_uri_patterns.match(path):
        return _PathType.cloud, path, None

    dataset_uri_pattern = re.compile(
        r'^azureml://locations/([^\/]+)/workspaces/([^\/]+)/data/([^\/]+)/versions/(.*)',
        re.IGNORECASE)
    dataset_uri_match = dataset_uri_pattern.match(path)
    if dataset_uri_match:
        return _PathType.legacy_dataset, path, (dataset_uri_match.group(3), dataset_uri_match.group(4))

    return _PathType.local, path, None
