import os
from mltable.mltable import from_json_lines_files
from .helper_functions import mltable_was_loaded, can_load_mltable
import pytest


@pytest.mark.mltable_sdk_unit_test
class TestFromJsonLines():
    def test_create_mltable_from_json_files_with_local_path(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        os.chdir(cwd)
        paths = [{'file': 'data/order.jsonl'}]
        mltable = from_json_lines_files(paths)
        df = mltable_was_loaded(mltable)
        assert df.shape == (3, 4)
        assert list(df.columns) == ['Timestamp',
                                    'IsOnline', 'ProductID', 'Price']

    def test_create_mltable_from_json_files_with_local_folder_path(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        os.chdir(cwd)
        paths = [{'folder': 'data/mltable/mltable_folder_jsonl'}]
        mltable = from_json_lines_files(paths)
        df = mltable_was_loaded(mltable)
        assert df.shape == (6, 4)
        assert list(df.columns) == ['Timestamp',
                                    'IsOnline', 'ProductID', 'Price']

    def test_create_mltable_from_json_files_with_local_paths(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        os.chdir(cwd)
        paths = [{'file': 'data/mltable/mltable_folder_jsonl/order.jsonl'},
                 {'file': 'data/mltable/mltable_folder_jsonl/order_2.jsonl'}]
        mltable = from_json_lines_files(paths)
        df = mltable_was_loaded(mltable)
        assert df.shape == (6, 4)
        assert list(df.columns) == ['Timestamp',
                                    'IsOnline', 'ProductID', 'Price']

    def test_create_mltable_from_json_files_with_local_abs_path(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/order.jsonl'))
        paths = [{'file': exp_path_1}]
        mltable = from_json_lines_files(paths)
        df = mltable_was_loaded(mltable)
        assert df.shape == (3, 4)
        assert list(df.columns) == ['Timestamp',
                                    'IsOnline', 'ProductID', 'Price']

    def test_from_json_lines_files_drop_invalid_rows(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/order_invalid.jsonl'))
        paths = [{'file': exp_path_1}]
        mltable = from_json_lines_files(paths, invalid_lines='drop')
        df = mltable.to_pandas_dataframe()
        assert df.shape == (2, 4)
        assert list(df.columns) == ['Timestamp',
                                    'IsOnline', 'ProductID', 'Price']

        mltable_paths = [os.path.join(
            cwd, "data/mltable/jsonlines_order_drop/")]
        for mltable_path in mltable_paths:
            df = can_load_mltable(mltable_path)
            assert df.shape == (2, 4)
            assert list(df.columns) == ['Timestamp',
                                        'IsOnline', 'ProductID', 'Price']

    def test_from_json_lines_files_error_on_invalid_rows(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/order_invalid.jsonl'))
        paths = [{'file': exp_path_1}]
        mltable = from_json_lines_files(paths, invalid_lines='error')
        with pytest.raises(Exception) as e:
            mltable.to_pandas_dataframe()
            print(e)

    def test_from_json_lines_files(self, get_dir_folder_path):
        wd = get_dir_folder_path
        mltable_paths = [os.path.join(
            wd, "data/mltable/jsonlines_order/")]
        for mltable_path in mltable_paths:
            df = can_load_mltable(mltable_path)
            assert df.shape == (3, 4)
            assert list(df.columns) == ['Timestamp',
                                        'IsOnline', 'ProductID', 'Price']
