from mltable._validation_and_error_handler import _classify_known_user_error, _download_error_handler, \
    _get_and_validate_download_list
from mltable.mltable import _get_logger
from azureml.dataprep.native import DataPrepError, StreamInfo
import pytest


@pytest.mark.mltable_sdk_unit_test
class TestValidationErrorHandler:
    def test_classify_known_user_error(self):
        exception_str_value_error = ["InvalidUriScheme", "StreamError(NotFound)", "DataAccessError(NotFound)",
                                     "No such host is known", "No identity was found on compute"]

        exception_str_system_error = ["Microsoft.DPrep.ErrorValues.PythonNumpyDatetimeParseFailure",
                                      "Microsoft.DPrep.ErrorValues.IntegerOverflow",
                                      "Microsoft.DPrep.ErrorValues.UnsupportedPythonObject"]

        with pytest.raises(ValueError) as e:
            err_string = 'ExecutionError(StreamError(PermissionDenied(Some(Server failed to authenticate the request'
            _classify_known_user_error(err_string)
        assert err_string in str(e.value)

        for val_error in exception_str_value_error:
            with pytest.raises(ValueError) as e:
                _classify_known_user_error(val_error)
            assert val_error in str(e.value)

        for sys_error in exception_str_system_error:
            with pytest.raises(SystemError) as e:
                _classify_known_user_error(sys_error)
            assert sys_error in str(e.value)

    def test_download_error_handler(self):
        value_error_list = [('dummyFile1.csv', "Microsoft.DPrep.ErrorValues.SourceFileNotFound"),
                            ('dummyFile2.csv', "Microsoft.DPrep.ErrorValues.SourceFilePermissionDenied"),
                            ('dummyFile3.csv', "Microsoft.DPrep.ErrorValues.InvalidArgument"),
                            ('dummyFile4.csv', "Microsoft.DPrep.ErrorValues.SourcePermissionDenied"),
                            ('dummyFile5.csv', "Microsoft.DPrep.ErrorValues.DestinationPermissionDenied"),
                            ('dummyFile6.csv', "Microsoft.DPrep.ErrorValues.DestinationDiskFull"),
                            ('dummyFile7.csv', "Microsoft.DPrep.ErrorValues.FileSizeChangedWhileDownloading"),
                            ('dummyFile8.csv', "Microsoft.DPrep.ErrorValues.StreamInfoInvalidPath"),
                            ('dummyFile9.csv', "Microsoft.DPrep.ErrorValues.NoManagedIdentity"),
                            ('dummyFile10.csv', "Microsoft.DPrep.ErrorValues.NoOboEndpoint"),
                            ('dummyFile11.csv', "Microsoft.DPrep.ErrorValues.StreamInfoRequired")]

        value_error_msg = 'Some files have failed to download:' + '\n'.join(
            [str((file_name, error_code)) for (file_name, error_code) in value_error_list])

        with pytest.raises(ValueError) as e:
            _download_error_handler(value_error_list)
        assert value_error_msg in str(e.value)

        with pytest.raises(RuntimeError) as e:
            _download_error_handler([('dummyFile.csv', 'Microsoft.DPrep.ErrorValues.IntegerOverflow')])
        assert "System error happens during downloading: Microsoft.DPrep.ErrorValues.IntegerOverflow" in str(e.value)

    def test_get_and_validate_download_list(self, get_data_folder_path):
        # Empty download_records
        empty_download_records = _get_and_validate_download_list([], False, _get_logger())
        assert empty_download_records == []

        download_records = [{'DestinationFile': DataPrepError("Microsoft.DPrep.ErrorValues.SourceFileNotFound",
                                                              originalValue="Path",
                                                              properties="")}]

        # Does not raise exception since the error is related to no file found in source thus returns empty list
        empty_download_list_not_present = _get_and_validate_download_list(download_records, True, _get_logger())
        assert empty_download_list_not_present == []

        # Error found in download_records
        download_records = [
            {'DestinationFile': DataPrepError(
                "Microsoft.DPrep.ErrorValues.InvalidArgument", originalValue="Path", properties="")}]
        with pytest.raises(ValueError) as e:
            _get_and_validate_download_list(download_records, False, _get_logger())
        assert "Some files have failed to download:" \
               "('Path', 'Microsoft.DPrep.ErrorValues.InvalidArgument')" in str(e.value)

        # Raises RuntimeError
        with pytest.raises(RuntimeError) as e:
            _get_and_validate_download_list([{'DestinationFile': 'expectingRuntimeError'}], False, _get_logger())
        assert "Unexpected error during file download" in str(e.value)

        # Happy path
        stream_info_object = StreamInfo(handler='Local', arguments={}, resource_identifier='C:/path/Titanic2.csv')
        download_records = [{'DestinationFile': stream_info_object}]
        download_records_happy_path = _get_and_validate_download_list(download_records, False, _get_logger())
        assert download_records_happy_path == ['C:/path/Titanic2.csv']
