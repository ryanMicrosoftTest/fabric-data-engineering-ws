import json
from ._aml_helper import get_workspace_from_run, verify_workspace

def _resolve_dataset(request, writer, socket):
    from azureml.core import Workspace
    try:
        from azureml.core import Dataset
    except ImportError:
        from azureml.contrib.dataset import Dataset

    ws_name = request.get('ws_name')
    subscription = request.get('subscription')
    resource_group = request.get('resource_group')
    if not ws_name or not subscription or not resource_group:
        writer.write(json.dumps({'result': 'error', 'error': 'InvalidWorkspace'}))
        return

    try:
        ws = get_workspace_from_run() or \
            Workspace.get(ws_name, subscription_id=subscription, resource_group=resource_group)
        verify_workspace(ws, subscription, resource_group, ws_name)
        dataset_name = request.get('dataset_name')
        dataset_version = request.get('dataset_version')
        definition = Dataset._get_definition_json(ws, dataset_name, dataset_version)
        writer.write(json.dumps({'result': 'success', 'data': definition}))
    except Exception as e:
        writer.write(json.dumps({'result': 'error', 'error': str(e)}))


def register_dataset_resolver(requests_channel):
    requests_channel.register_handler('resolve_dataset', _resolve_dataset)


def is_dataset(dataset):
    try:
        from azureml.core import Dataset
        from azureml.data.dataset_definition import DatasetDefinition
    except ImportError:
        try:
            from azureml.contrib.dataset import Dataset, DatasetDefinition
        except ImportError:
            return False

    return isinstance(dataset, Dataset) or isinstance(dataset, DatasetDefinition)


def reference(dset):
    from .references import ExternalReference
    try:
        from azureml.core import Dataset
        from azureml.data.dataset_definition import DatasetDefinition
    except ImportError:
        from azureml.contrib.dataset import Dataset, DatasetDefinition

    if not isinstance(dset, Dataset) and not isinstance(dset, DatasetDefinition):
        raise ValueError('Reference can only be made to dataset but got {} instead'.format(type(dset)))
    if (isinstance(dset, DatasetDefinition) and not (dset._workspace and dset._dataset_id)) \
            or (isinstance(dset, Dataset) and not (dset.workspace and dset.id)):
        raise ValueError('Cannot reference an in-memory dataset')

    dataset = None
    definition = None

    if isinstance(dset, Dataset):
        dataset = dset
    else:
        definition = dset
        try:
            dataset = dset._dataset or Dataset.get(dset._workspace, id=dset._dataset_id)
        except AttributeError:
            # DatasetDefinition#_dataset is only available after dataset moved into AzureML Core
            dataset = Dataset.get(dset._workspace, id=dset._dataset_id)

    paths = [
        dataset.workspace.subscription_id,
        dataset.workspace.resource_group,
        dataset.workspace.name,
        dataset.name
    ]
    if definition:
        paths.append(definition._version_id)

    return ExternalReference(package_path='dataset://{}'.format('/'.join(paths)))
