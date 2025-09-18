# Copyright (c) Microsoft Corporation. All rights reserved.
from .engineapi.api import get_engine_api
from .engineapi.typedefinitions import (ActivityReference, ActivityReferenceType,
                                        CreateAnonymousReferenceMessageArguments)
from .step import steps_to_block_datas
from ... import dataprep


def _to_anonymous_reference(df: 'dataprep.Dataflow') -> ActivityReference:
    blocks = steps_to_block_datas(df._steps)
    return get_engine_api().create_anonymous_reference(
        CreateAnonymousReferenceMessageArguments(blocks))


class ExternalReference:
    """
    A reference to a Dataflow that is saved to a file.

    :param package_path: Path to the referenced DataPrep package.
    """
    def __init__(self, package_path: str):
        self._package_path = package_path


def make_activity_reference(reference: 'dataprep.DataflowReference') -> ActivityReference:
    from .dataflow import Dataflow
    from ._dataset_resolver import is_dataset, reference as reference_dataset

    if is_dataset(reference):
        external_ref = reference_dataset(reference)
        return ActivityReference(reference_container_path=external_ref._package_path,
                                 reference_type=ActivityReferenceType.FILE)
    elif isinstance(reference, Dataflow):
        return _to_anonymous_reference(reference)
    elif isinstance(reference, ExternalReference):
        return ActivityReference(reference_container_path=reference._package_path,
                                 reference_type=ActivityReferenceType.FILE)
    else:
        raise TypeError('Invalid type for Dataflow reference.')
