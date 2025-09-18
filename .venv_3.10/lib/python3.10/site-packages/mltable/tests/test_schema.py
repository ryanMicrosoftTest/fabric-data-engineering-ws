# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
import pytest
import tempfile
import yaml
from mltable.mltable import DataType, _read_yaml, from_delimited_files, from_parquet_files, from_delta_lake, \
    from_json_lines_files


@pytest.mark.mltable_sdk_unit_test
class TestSchema:
    """Checks that parts of a MLTable YAML file (transformation steps, metadata, traits, etc.) that can be added the
    MLTable API (public or private methods) and then saved are apart of the MLTable JSON schema. Doesn't exhaustively
    check all configurations under each option, just the high level inclusion.
    """

    def _check_mltable_yaml_sections_equal(self, og_yaml, new_yaml, section):
        if section in og_yaml:
            assert og_yaml[section] == new_yaml[section]
        else:
            assert section not in og_yaml
            assert section not in new_yaml

    def _check_mltable_passes_schema(self, mltable):
        with tempfile.TemporaryDirectory() as td:
            og_yaml_rep = yaml.safe_load(str(mltable))
            mltable.save(td)
            loaded_yaml_rep = _read_yaml(td)  # if loads successfully, schema passes

            # check that YAML is same before & after saving, anre't strictly necessary but good sanity check
            # don't check `paths` section as `MLTable.save` can change local relative file paths
            for section in 'transformation', 'metadata', 'traits':
                self._check_mltable_yaml_sections_equal(og_yaml_rep, loaded_yaml_rep, section)

    def test_load(self, get_mltable):
        # `MLTable.load` may sometimes add extra transformation steps, called with `get_mltable` fixture
        self._check_mltable_passes_schema(get_mltable)

    def test_from_read_parquet(self):
        self._check_mltable_passes_schema(from_parquet_files([{'file': 'sample.parquet'}]))

    def test_from_delimited_files(self):
        self._check_mltable_passes_schema(from_delimited_files([{'file': 'sample.csv'}]))

    def test_from_json_lines_files(self):
        self._check_mltable_passes_schema(from_json_lines_files([{'file': 'sample.jsonl'}]))

    def test_from_delta_lake(self):
        self._check_mltable_passes_schema(from_delta_lake('test-delta'))

    def test_select_partitions(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.select_partitions([1, 2]))

    def test_extract_columns_from_partition_format(self, empty_mltable):
        self._check_mltable_passes_schema(
            empty_mltable.extract_columns_from_partition_format('{column_name:yyyy/MM/dd/HH/mm/ss}'))

    def test_filter(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.filter('col("FBI Code") == \"11\"'))

    def test_take(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.take())

    def test_take_random_sample(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.take_random_sample(.5))

    def test_drop_columns(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.drop_columns('foo'))

    def test_keep_columns(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.keep_columns('foo'))

    def test_random_split(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.random_split()[0])

    def test_skip(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.skip(10))

    def test_convert_column_types(self, empty_mltable):
        self._check_mltable_passes_schema(empty_mltable.convert_column_types({'foo': DataType.to_string()}))

    def test__with_partition_size(self, empty_mltable):
        # require `read_delimited` or `read_json_files` transformation step to call `with_partition_size`
        self._check_mltable_passes_schema(
            from_delimited_files([{'file': 'sample.csv'}])._with_partition_size(min_batch_size=1))

    def test_metadata(self, empty_mltable):
        empty_mltable.metadata.add('key', 'value')
        self._check_mltable_passes_schema(empty_mltable)

    def test_traits(self, empty_mltable):
        empty_mltable.traits.index_columns = ['foo']
        self._check_mltable_passes_schema(empty_mltable)
