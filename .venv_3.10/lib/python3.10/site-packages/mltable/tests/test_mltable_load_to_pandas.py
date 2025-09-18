import os
from mltable.mltable import load, from_delimited_files, from_parquet_files, _load
from .helper_functions import mltable_was_loaded, can_load_mltable
import pandas as pd
import pytest


@pytest.mark.mltable_sdk_unit_test
class TestMLTable_load_to_pandas:
    def test_to_pandas_dataframe_from_mltable(self, get_mltable):
        mltable = get_mltable
        pdf = mltable_was_loaded(mltable)
        assert pdf.shape == (40, 12)

    def test_extract_partition_format_into_columns_using_sdk(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format')
        mltable = load(test_mltable_dir)
        partition_format = '{file_type}/{file_name}.txt'
        mltable = mltable.extract_columns_from_partition_format(
            partition_format)
        df = mltable.to_pandas_dataframe()
        assert df.shape == (4, 3)
        assert df['file_name'].tolist() == ['cat1', 'cat2', 'dog1', 'dog2']
        assert df['file_type'].tolist() == ['cats', 'cats', 'dogs', 'dogs']

    def test_partition_keys(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format')
        mltable = load(test_mltable_dir)
        partition_format = '{file_type}/{file_name}.txt'
        mltable = mltable.extract_columns_from_partition_format(
            partition_format)
        assert len(mltable.partition_keys) == 2

    def test_get_partition_key_values_with_default_input(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format')
        mltable = load(test_mltable_dir)
        partition_format = '{file_type}/{file_name}.txt'
        mltable = mltable.extract_columns_from_partition_format(
            partition_format)
        expected_result = [{'file_name': 'cat1', 'file_type': 'cats'},
                           {'file_name': 'cat2', 'file_type': 'cats'},
                           {'file_name': 'dog1', 'file_type': 'dogs'},
                           {'file_name': 'dog2', 'file_type': 'dogs'}]
        assert sorted(mltable._get_partition_key_values(), key=lambda ele: sorted(ele.items())) == \
            expected_result

    def test_get_partition_key_values_with_invalid_key(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format')
        mltable = load(test_mltable_dir)
        partition_format = '{file_type}/{file_name}.txt'
        mltable = mltable.extract_columns_from_partition_format(
            partition_format)
        with pytest.raises(Exception) as e:
            mltable._get_partition_key_values(['Invalid_key'])
        assert str(e.value) == "['Invalid_key'] are invalid partition keys"

    # this test could be added back after distinct step is added.
    def test_get_partition_key_values_with_key_name(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format')
        mltable = load(test_mltable_dir)
        partition_format = '{file_type}/{file_name}.txt'
        mltable = mltable.extract_columns_from_partition_format(
            partition_format)
        expected_result = [{'file_type': 'cats'}, {'file_type': 'dogs'}]
        assert sorted(mltable._get_partition_key_values(['file_type']), key=lambda ele: sorted(ele.items())) == \
            expected_result

    def test_mltable_filter_complex(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/train.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths, infer_column_types=False)
        filtered_mltable = mltable.filter('feature_1 == \"5\" and feature_2 == \"2\" and feature_3 == \"1\" '
                                          'and target > \"0.5)\"')
        df = mltable_was_loaded(filtered_mltable)
        assert df.shape == (4929, 6)

    def test_mltable_filter(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime-spring.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths, infer_column_types=False)
        filtered_mltable = mltable.filter('ID == \"10522293\"')
        df = mltable_was_loaded(filtered_mltable)
        assert df.shape == (1, 22)

    def test_mltable_load_with_filter(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/load_filter')
        mltable = load(test_mltable_dir)
        df = mltable_was_loaded(mltable)
        assert df.shape == (1, 22)

    def test_mltable_filter_column_space(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime-spring.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths, infer_column_types=False)
        filtered_mltable = mltable.filter('col("FBI Code") == \"11\"')
        df = mltable_was_loaded(filtered_mltable)
        assert df.shape == (6, 22)

    def test_mltable_filter_start_with(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime-spring.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths)
        filtered_mltable = mltable.filter(
            'Description.startswith(\"FINANCIAL\")')
        df = mltable_was_loaded(filtered_mltable)
        assert df.shape == (4, 22)

    def test_mltable_filter_datetime(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime.parquet'))
        paths = [{'file': exp_path_1}]
        mltable = from_parquet_files(paths)
        filtered_mltable = mltable.filter('Date >= datetime(2015, 7, 5, 23)')
        df = mltable_was_loaded(filtered_mltable)
        assert df.shape == (5, 22)

    def test_mltable_filter_column_invalid_expression(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime-spring.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths, infer_column_types=False)
        expression = 'FBI Code == \"11\"'
        filtered_mltable = mltable.filter(expression)
        with pytest.raises(Exception) as e:
            mltable_was_loaded(filtered_mltable)
        assert 'Not a valid python expression in filter' in str(e.value)

    def test_extract_partition_format_into_columns_using_mltable_file(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        test_mltable_dir = os.path.join(
            cwd, 'data/mltable/partition_format_specified')
        mltable = load(test_mltable_dir)
        df = mltable.to_pandas_dataframe()
        assert df.shape == (9, 7)
        assert df['file_name'].tolist() == ['A', 'A', 'A', 'C',
                                            'C', 'C', 'B', 'B', 'B']
        assert df['Date'].tolist() == [
            pd.Timestamp(2021, 3, 25, 4, 7, 45),
            pd.Timestamp(2021, 3, 25, 4, 7, 45),
            pd.Timestamp(2021, 3, 25, 4, 7, 45),
            pd.Timestamp(2021, 3, 25, 18, 10, 59),
            pd.Timestamp(2021, 3, 25, 18, 10, 59),
            pd.Timestamp(2021, 3, 25, 18, 10, 59),
            pd.Timestamp(2021, 5, 28, 6, 27, 19),
            pd.Timestamp(2021, 5, 28, 6, 27, 19),
            pd.Timestamp(2021, 5, 28, 6, 27, 19)
        ]
        assert df['extension'].tolist() == ['csv', 'csv', 'csv', 'csv',
                                            'csv', 'csv', 'csv', 'csv', 'csv']

    def test_same_paths_after_added_step(self, get_mltable):
        mltable = get_mltable
        assert mltable.paths == mltable.take(5).paths

    def test_mltable_dataflow(self, get_dir_folder_path):
        dir_path = get_dir_folder_path
        data_folder_path = os.path.join(
            dir_path, 'data/mltable/mltable_tabular')
        mltable = load(data_folder_path)
        # TODO update to assert on yaml string should be the same with the mltable yaml thats loaded
        print(mltable._dataflow.to_yaml_string())
        pd = mltable.to_pandas_dataframe()
        assert pd.shape == (891, 12)

    def test_traits_not_in_runtime_needed_props(self, get_dir_folder_path):
        dir_path = get_dir_folder_path
        data_folder_path = os.path.join(
            dir_path, 'data/mltable/traits_timeseries')
        df = can_load_mltable(data_folder_path)
        assert df.shape == (8761, 16)

    def test_load_execute(self, get_dir_folder_path):
        dir_path = get_dir_folder_path
        # write to preppy
        data_folder_path = os.path.join(
            dir_path, 'data/mltable/preppy/write_preppy')
        mltable = _load(data_folder_path)
        mltable._execute()

        # read from preppy
        data_folder_path_read = os.path.join(
            dir_path, 'data/mltable/preppy/read_preppy')
        mltable_read_preppy = _load(data_folder_path_read)
        mltable_read_preppy._execute()
        mltable_was_loaded(mltable_read_preppy)
