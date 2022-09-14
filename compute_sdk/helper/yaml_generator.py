import io
import json

import yaml

from compute_sdk.entities.request.compute_request import ComputeDefinition
from compute_sdk.entities.request.worker_group import WorkerGroup
import importlib.resources as pkg_resource
import compute_sdk.helper.resources as rs


def read_yaml():
    return


def create_yaml(compute_request: ComputeDefinition):
    with pkg_resource.open_text(rs, 'values.yaml') as stream:
        loaded = yaml.safe_load(stream)

    loaded['podTypes']['rayHeadType']['CPU'] = compute_request.head_node.head_node_cores
    loaded['podTypes']['rayHeadType']['memory'] = str(compute_request.head_node.head_node_memory) + 'Gi'

    worker_node_list = compute_request.worker_group
    worker_group: WorkerGroup = worker_node_list[0]

    loaded['podTypes']['rayWorkerType']['minWorkers'] = worker_group.min_pods
    loaded['podTypes']['rayWorkerType']['maxWorkers'] = worker_group.max_pods
    loaded['podTypes']['rayWorkerType']['memory'] = str(worker_group.memory) + 'Gi'
    loaded['podTypes']['rayWorkerType']['CPU'] = worker_group.cores
    new_file = io.StringIO(json.dumps(loaded))
    # print(json.dumps(loaded))
    return new_file
