import copy
import os.path

import tempfile
import yaml
from azureml.dataprep import _read_yaml

from mltable.mltable import load


def mltable_was_loaded(mltable):
    df = mltable.to_pandas_dataframe()
    assert df is not None
    assert not df.empty
    return df


def can_load_mltable(uri, storage_options=None):
    try:
        mltable = load(uri=uri, storage_options=storage_options)
    except Exception as e:
        assert False, f'failed to load MLTable, got error [{type(e)}: {e}]'

    _test_save_load_round_trip(mltable, storage_options)
    return mltable_was_loaded(mltable)


def _test_save_load_round_trip(mltable, storage_options):
    with tempfile.TemporaryDirectory() as temp_dir:
        mltable.save(temp_dir)
        loaded_mltable = load(temp_dir, storage_options=storage_options)
        original_df = mltable.to_pandas_dataframe()
        loaded_df = loaded_mltable.to_pandas_dataframe()
        assert original_df is not None
        assert loaded_df is not None
        assert original_df.equals(loaded_df)
        # test that the string representation remains the same after roundtrip
        assert str(mltable) == str(loaded_mltable)


def mltable_as_dict(mltable):
    """
    Given a MLTable, returns it's underlying Dataflow (added transformation steps, metadata, etc.)
    as a dictionary
    """
    return yaml.safe_load(mltable._dataflow.to_yaml_string())


def get_mltable_and_dicts(path):
    mltable = load(path)
    return mltable, mltable_as_dict(mltable), _read_yaml(path)


def get_invalid_mltable(get_invalid_data_folder_path):
    return load(get_invalid_data_folder_path)


def save_mltable_yaml_dict(save_dirc, mltable_yaml_dict):
    save_path = os.path.join(save_dirc, 'MLTable')
    with open(save_path, 'w') as f:
        yaml.safe_dump(mltable_yaml_dict, f)
    return save_path


def list_of_dicts_equal(a, b, c=None):
    b = copy.deepcopy(b)
    assert len(a) == len(b)
    for x in a:
        b.remove(x)
    assert len(b) == 0

    if c is not None:
        list_of_dicts_equal(a, c)
