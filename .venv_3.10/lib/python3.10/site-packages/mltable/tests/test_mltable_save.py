import os
import tempfile

import pytest
from azureml.dataprep.api.mltable._mltable_helper import _read_yaml

from .helper_functions import mltable_as_dict, save_mltable_yaml_dict, list_of_dicts_equal
from mltable.mltable import load


@pytest.mark.mltable_sdk_unit_test
class TestMLTableSave:
    def test_save(self, get_mltable_data_folder_path):
        mltable_path = get_mltable_data_folder_path
        og_mltable = load(mltable_path)

        with tempfile.TemporaryDirectory() as tf:
            og_mltable.save(tf)
            og_mltable_yaml_dict = mltable_as_dict(og_mltable)
            saved_mltable_yaml_file_dict = _read_yaml(tf)

            # paths in MLTable's Dataflow are saved as expected
            for path_dict in og_mltable_yaml_dict['paths']:
                for path_prop, path in path_dict.items():
                    path = path[7:]  # remove 'file://'
                    assert os.path.isabs(path)  # paths are saved to non-relative directory so saved as absolute paths
                    assert {path_prop: path} in saved_mltable_yaml_file_dict['paths']
                    assert os.path.exists(path)

    def test_save_relative_directory(self, get_mltable_data_folder_path):
        # mock saving MLTable to adjacent directory
        mltable_path = get_mltable_data_folder_path
        mltable_yaml_dict = _read_yaml(mltable_path)

        with tempfile.TemporaryDirectory() as td:
            a_dirc = os.path.join(td, 'a')
            os.mkdir(a_dirc)

            b_dirc = os.path.join(td, 'b')
            os.mkdir(b_dirc)

            # set up fake MLTable file
            save_mltable_yaml_dict(a_dirc, mltable_yaml_dict)
            og_mltable = load(a_dirc)
            og_mltable.save(b_dirc)

            og_mltable_yaml_dict = mltable_as_dict(og_mltable)
            saved_mltable_yaml_file_dict = _read_yaml(b_dirc)

            # paths in MLTable's Dataflow are saved as expected
            for path_dict in og_mltable_yaml_dict['paths']:
                for path_prop, path in path_dict.items():
                    path = path[7:]  # remove 'file://'

                    # paths in MLTable's Dataflow are absolute
                    assert os.path.isabs(path)

                    # MLTable is saved to relative adjacent directory, so paths are saved as relative
                    path = os.path.relpath(path, b_dirc)
                    assert not os.path.isabs(path)
                    assert {path_prop: path} in saved_mltable_yaml_file_dict['paths']

    def test_save_to_existing_dirc_with_mltable_overwrite_true(self, get_mltable):
        mltable = get_mltable
        with tempfile.TemporaryDirectory() as save_dirc:
            save_path = os.path.join(save_dirc, 'MLTable')

            mltable.save(save_dirc)
            assert os.path.exists(save_path)

            mltable.save(save_dirc, overwrite=True)
            assert os.path.exists(save_path)

    def test_save_to_existing_dirc_with_mltable_overwrite_false(self, get_mltable_data_folder_path):
        # try to save to directory that has a MLTable file
        # in this case the same directory the MLTable was originally loaded
        mltable_path = get_mltable_data_folder_path
        existing_mltable_save_path = os.path.join(mltable_path, 'MLTable')
        assert os.path.exists(existing_mltable_save_path)
        mltable = load(mltable_path)
        with pytest.raises(ValueError):
            mltable.save(mltable_path, overwrite=False)

    def test_save_to_file_path(self, get_mltable):
        # try to save to *existing* file path
        mltable = get_mltable
        with tempfile.TemporaryDirectory() as save_dirc:
            save_path = os.path.join(save_dirc, 'foo.yml')
            assert not os.path.exists(save_path)
            with open(save_path, 'w') as f:
                f.write('foo')
            assert os.path.isfile(save_path)

            with pytest.raises(ValueError):
                mltable.save(save_path)  # try to save to file


@pytest.mark.mltable_sdk_unit_test_windows
class TestMLTableSaveWindowsOnly:
    def test_save_diff_mount(self, get_data_folder_path):
        mltable_dirc_path = get_data_folder_path
        mltable_path = os.path.join(mltable_dirc_path, 'mltable_windows')
        og_mltable = load(mltable_path)

        with tempfile.TemporaryDirectory() as td:
            og_mltable.save(td)
            og_mltable_yaml_dict = mltable_as_dict(og_mltable)
            saved_mltable_yaml_file_dict = _read_yaml(td)

            # original MLTable directory & save directory are on different mounts
            dirc_mount = os.path.splitdrive(td)[0]
            for path_dirc in og_mltable_yaml_dict['paths']:
                assert os.path.splitdrive(path_dirc['file'])[0] != dirc_mount

            # all paths are absolute paths whose mount in the original MLTable direcotry
            exp_save_paths = [{'file': 'D:\\absolute\\path\\file.csv'},
                              {'file': os.path.join(mltable_path, 'relative\\path\\file.csv')}]

            # explit check that saved paths are the same
            list_of_dicts_equal(exp_save_paths, saved_mltable_yaml_file_dict['paths'])

    def test_save_same_mount(self, get_data_folder_path):
        mltable_dirc_path = get_data_folder_path
        mltable_path = os.path.join(mltable_dirc_path, 'mltable_windows_c_drive')
        mltable_yaml_dirc = _read_yaml(mltable_path)

        with tempfile.TemporaryDirectory() as td:
            a_dirc = os.path.join(td, 'a')
            os.mkdir(a_dirc)

            b_dirc = os.path.join(td, 'b')
            os.mkdir(b_dirc)

            # setup fake MLTable
            save_mltable_yaml_dict(a_dirc, mltable_yaml_dirc)
            og_mltable = load(a_dirc)
            og_mltable.save(b_dirc)

            og_mltable_yaml_dict = mltable_as_dict(og_mltable)
            saved_mltable_yaml_file_dict = _read_yaml(b_dirc)

            # original MLTable directory & save directory are on same mounts
            dirc_mount = os.path.splitdrive(td)[0]
            for path_dirc in og_mltable_yaml_dict['paths']:
                if os.path.isabs(path_dirc['file']):
                    assert os.path.splitdrive(path_dirc['file'])[0] == dirc_mount

            exp_save_paths = [{'file': os.path.relpath('C:\\absolute\\path\\file.csv', b_dirc)},
                              {'file': '..\\a\\relative\\path\\file.csv'}]

            # explit check that saved paths are the same
            list_of_dicts_equal(exp_save_paths, saved_mltable_yaml_file_dict['paths'])
