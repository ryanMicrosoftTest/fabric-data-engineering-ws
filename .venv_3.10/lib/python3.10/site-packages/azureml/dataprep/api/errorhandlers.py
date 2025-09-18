# Copyright (c) Microsoft Corporation. All rights reserved.
# pylint: disable=line-too-long
from ._loggerfactory import session_id
import os


def raise_engine_error(error_response):
    error_code = error_response['errorCode']
    if 'ScriptExecution' in error_code:
        raise ExecutionError(error_response)
    if 'Validation' in error_code:
        raise ValidationError(error_response)
    if 'StepTranslation' in error_code:
        raise ValidationError(error_response)
    elif 'UnableToPreviewDataSource' in error_code:
        raise ExecutionError(error_response)
    elif 'EmptySteps' in error_code:
        raise EmptyStepsError()
    elif 'OperationCanceled' in error_code:
        raise OperationCanceled()
    else:
        raise UnexpectedError(error_response)


class DataPrepException(Exception):
    def __init__(self, message, error_code, compliant_message, error_data = None):
        self.error_code = error_code if error_code is not None else 'Unexpected'
        self.compliant_message = compliant_message + '| session_id={}'.format(session_id)
        self.message = message + '| session_id={}'.format(session_id)
        self.error_data = error_data
        super().__init__(self.message)

    def __repr__(self) -> str:
        """
        Return string representation of the exception.
        """
        return "\nError Code: {}".format(self.error_code) + \
            "\nError Message: {}".format(self.message)

    def __str__(self) -> str:
        return self.__repr__()


class OperationCanceled(DataPrepException):
    """
    Exception raised when an execution has been canceled.
    """
    def __init__(self):
        super().__init__('The operation has been canceled.', 'Canceled', 'The operation has been canceled.')


class StorageAccountLimit(DataPrepException):
    """
    Exception raised when dataflow execution fails due to exceeded storage account limits.
    """
    def __init__(self, error_message):
        error_code = 'ScriptExecution.StreamAccess.Throttling'

        super().__init__(error_message, error_code, error_message)

    def __repr__(self) -> str:
        """
        Return string representation of the exception.
        """
        return "\nError Code: {}".format(self.error_code) + \
            "\nExecution has failed due to exceeded storage account limits. Consider upgrading storage account to higher performance tier or distributing data access across several storage accounts." + \
            "\nRaw error Message: {}".format(self.message)

    def __str__(self) -> str:
        return self.__repr__()


class ExecutionError(DataPrepException):
    """
    Exception raised when dataflow execution fails.
    """
    def __init__(self, error_response, error_code = None, error_message = None, compliant_message = None, error_data = None, native_error = None):
        if error_response is not None:
            self.outer_error_code = error_response['errorData'].get('outerErrorCode', None) # identity of outer error (including Dependency layers)
            self.step_failed = error_response['errorData'].get('stepFailed', None)
            # if execution error is caused by ValidationError we will get those properties set
            self.validation_target = error_response['errorData'].get('validationTarget', None)
            self.validation_error_code = error_response['errorData'].get('validationErrorCode', None)
            self.native_error = None
            error_code = error_response.get('errorCode', None) # identity of the the root error
            error_message = error_response.get('message', '')
            compliant_message = error_response['errorData'].get('loggingErrorMessage', '')
            error_data = error_response['errorData']
            super().__init__(error_message, error_code, compliant_message, error_data)
        else:
            self.outer_error_code = None
            self.step_failed = None
            self.validation_target = None
            self.validation_error_code = None
            self.native_error = native_error
            super().__init__(error_message, error_code, compliant_message, error_data)

    def __repr__(self) -> str:
        """
        Return string representation of the exception.
        """
        if self.native_error is None:
            return "\nError Code: {}".format(self.error_code) + \
                ("\nOuter Error Code: {}".format(self.outer_error_code) if self.outer_error_code != self.error_code else '') + \
                ("\nValidation Error Code: {}".format(self.validation_error_code) if self.validation_target is not None else '')+ \
                ("\nValidation Target: {}".format(self.validation_target) if self.validation_target is not None else '') + \
                ("\nFailed Step: {}".format(self.step_failed) if self.step_failed is not None else '') + \
                "\nError Message: {}".format(self.message)
        else:
            return "\nError Code: {}".format(self.error_code) + \
                "\nNative Error: {}".format(self.native_error) + \
                "\nError Message: {}".format(self.message)

    def __str__(self) -> str:
        return self.__repr__()


class ValidationError(DataPrepException):
    """
    Exception raised when dataflow execution fails.
    """
    def __init__(self, error_response, validation_target = None, validation_error_code = None, error_code = None, error_message = None, compliant_message = None, error_data = None, native_error = None):
        if error_response is not None:
            self.step_failed = error_response['errorData'].get('stepFailed', None)
            self.step_failed_type = error_response['errorData'].get('stepFailedType', None)
            self.validation_target = error_response['errorData'].get('validationTarget', None)
            self.validation_error_code = error_response['errorData'].get('validationErrorCode', None)
            self.native_error = None
            error_code = error_response.get('errorCode', None) # identity of the the root error   
            error_message = error_response.get('message', '')
            compliant_message = error_response['errorData'].get('loggingErrorMessage', '')
            super().__init__(error_message, error_code, compliant_message, error_response['errorData'])
        else:
            self.outer_error_code = None
            self.step_failed = None
            self.step_failed_type = None
            self.validation_target = validation_target
            self.validation_error_code = validation_error_code
            self.native_error = native_error
            super().__init__(error_message, error_code, compliant_message, error_data)

    def __repr__(self) -> str:
        """
        Return string representation of the exception.
        """
        if self.native_error is None:
            return "\nError Code: {}".format(self.error_code) + \
                "\nValidation Error Code: {}".format(self.validation_error_code) + \
                "\nValidation Target: {}".format(self.validation_target) + \
                ("\nFailed Step: {}".format(self.step_failed) if self.step_failed is not None else '') + \
                ("\nFailed Step Type: {}".format(self.step_failed_type) if self.step_failed_type is not None else '') + \
                "\nError Message: {}".format(self.message)
        else:
            return "\nError Code: {}".format(self.error_code) + \
                "\nValidation Error Code: {}".format(self.validation_error_code) + \
                "\nValidation Target: {}".format(self.validation_target) + \
                "\nNative error: {}".format(self.native_error) + \
                "\nError Message: {}".format(self.message)


    def __str__(self) -> str:
        return self.__repr__()


class EmptyStepsError(DataPrepException):
    """
    Exception raised when there are issues with steps in the dataflow.
    """
    def __init__(self):
        message = 'The Dataflow contains no steps and cannot be executed. Use a reader to create a Dataflow that can load data.'
        super().__init__(message, "EmptySteps", message)


class UnexpectedError(DataPrepException):
    """
    Unexpected error.

    :var error: Error code of the failure.
    """
    def __init__(self, error_response, compliant_message = None):
        super().__init__(str(error_response), 'UnexpectedFailure', compliant_message or '[REDACTED]')


class PandasImportError(DataPrepException):
    """
    Exception raised when pandas was not able to be imported.
    """
    _message = 'Could not import pandas. Ensure a compatible version is installed by running: pip install azureml-dataprep[pandas]'
    def __init__(self):
        print('PandasImportError: ' + self._message)
        super().__init__(self._message, 'PandasImportError', self._message)


class NumpyImportError(DataPrepException):
    """
    Exception raised when numpy was not able to be imported.
    """
    _message = 'Could not import numpy. Ensure a compatible version is installed by running: pip install azureml-dataprep[pandas]'
    def __init__(self):
        super().__init__(self._message, 'NumpyImportError', self._message)
