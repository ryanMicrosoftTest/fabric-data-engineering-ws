# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import os
import sys
from mltable.mltable import load, MLTable
import pytest
from os import path

# Add this directory to the path so tests don't need to change the path
# before importing common code
curdir = path.dirname(path.abspath(__file__))
sys.path.append(path.normpath(path.join(curdir, "../../../../tests/scenarios")))

pytest_plugins = "utilities.plugins.output_handler", \
                 "utilities.plugins.enforce_owner", \
                 "utilities.plugins.reporting"


def pytest_itemcollected(item):
    from utilities import constants
    if not item.get_closest_marker("owner"):
        item.add_marker(pytest.mark.owner(email=constants.owner_email_data_service))


@pytest.fixture(scope="session")
def test_suite_prefix():
    from utilities import constants
    return constants.data_service_short_name


@pytest.fixture(scope='module')
def get_dir_folder_path():
    return path.dirname(path.realpath(__file__))


@pytest.fixture(scope='module', autouse=True)
def get_dataset_data_folder_path(get_dir_folder_path):
    data_folder_path = os.path.realpath(os.path.join(get_dir_folder_path, 'data/'))
    return data_folder_path


@pytest.fixture(scope='module')
def get_data_folder_path(get_dir_folder_path):
    return os.path.realpath(os.path.join(get_dir_folder_path, 'data/mltable/'))


@pytest.fixture(scope='module')
def get_mltable_data_folder_path(get_data_folder_path):
    return os.path.join(get_data_folder_path, 'mltable_relative')


@pytest.fixture(scope='module')
def get_invalid_data_folder_path(get_data_folder_path):
    return os.path.join(get_data_folder_path, 'mltable_invalid')


@pytest.fixture(scope='module')
def get_mltable(get_mltable_data_folder_path):
    return load(get_mltable_data_folder_path)


@pytest.fixture(scope='module')
def empty_mltable():
    return MLTable._create_from_dict({'paths': [{'file': 'foo.csv'}]}, None)
