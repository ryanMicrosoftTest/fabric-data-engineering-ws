import os
from mltable.mltable import from_delimited_files, from_json_lines_files
from .helper_functions import mltable_was_loaded
import pytest


@pytest.mark.mltable_sdk_unit_test
class TestPartitionApi:
    def test_get_partition_count_json(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/order.jsonl'))
        paths = [{'file': exp_path_1}]
        mltable = from_json_lines_files(paths)

        previous_partition_count = mltable.get_partition_count()
        assert previous_partition_count == 1

        mini_batch_size = 200
        mltable = mltable._with_partition_size(mini_batch_size)
        new_partition_count = mltable.get_partition_count()
        assert new_partition_count == 2

    def test_select_partition(self, get_dir_folder_path):
        cwd = get_dir_folder_path
        exp_path_1 = os.path.normpath(
            os.path.join(cwd, 'data/crime-spring.csv'))
        paths = [{'file': exp_path_1}]
        mltable = from_delimited_files(paths)

        mini_batch_size = 200
        mltable = mltable._with_partition_size(mini_batch_size)
        new_partition_count = mltable.get_partition_count()
        assert new_partition_count == 11

        partition_index_list = [1, 2]
        mltable = mltable.select_partitions(partition_index_list)
        df = mltable_was_loaded(mltable)
        assert df.shape == (2, 22)
