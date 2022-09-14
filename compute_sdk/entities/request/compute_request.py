import json
from dataclasses import *
from typing import List

from dataclasses_json import DataClassJsonMixin
from mlp_commons.models.identifier import Identifier
from mlp_commons.models.meta_data import MetaData
from mlp_commons.models.search_query import SearchQuery
from typeguard import typechecked

from compute_sdk.entities.request.advance_config import AdvanceConfig
from compute_sdk.entities.request.head_node import HeadNode
from compute_sdk.entities.request.worker_group import WorkerGroup


@dataclass
@typechecked()
class ComputeDefinition(MetaData, DataClassJsonMixin):
    """
    A Compute Request represents a class for compute request.

    Args:
        name: Name of the compute.
        tags: List of the tags,
        runtime:  example=R10:R1.1.1,TF 1.2
        terminate_after_minutes: Time after which compute will be terminated
        infra_template: Template for compute specification
        head_node: Head Node
        worker_group: Worker group configuration of compute,
        advance_config: Advance configuration for compute
    """
    name: str
    tags: List[str]
    runtime: str
    terminate_after_minutes: int
    infra_template: str
    head_node: HeadNode
    worker_group: List[WorkerGroup]
    advance_config: AdvanceConfig
    cluster_id: str = ""
    user: str = ""
    created_on: str = ""

    def __init__(
            self,
            name: str,
            tags: List[str],
            runtime: str,
            terminate_after_minutes: int,
            infra_template: str,
            head_node: HeadNode,
            worker_group: List[WorkerGroup],
            advance_config: AdvanceConfig,
            user: str = "",
            created_on: str = "",
            cluster_id: str = ""
    ):

        if not isinstance(name, str):
            raise ValueError("name = %s is not a valid input" % name)
        self.name = name
        if not isinstance(tags, List):
            raise ValueError("tags is not a valid input")
        self.tags = tags
        if not isinstance(runtime, str):
            raise ValueError("runtime = %s is not a valid input" % runtime)
        self.runtime = runtime
        if not isinstance(terminate_after_minutes, int):
            raise ValueError("terminate_after_minutes = %s is not a valid input" % terminate_after_minutes)
        self.terminate_after_minutes = terminate_after_minutes
        if not isinstance(infra_template, str):
            raise ValueError("infra_template is not a valid input")
        self.infra_template = infra_template
        if not isinstance(head_node, HeadNode):
            raise ValueError("head_node is not a valid input")
        self.head_node = head_node
        if not isinstance(worker_group, List):
            raise ValueError("worker_group is not a valid input")
        self.worker_group = worker_group
        if not isinstance(advance_config, AdvanceConfig):
            raise ValueError("advanceConfig is not a valid input")
        self.advance_config = advance_config
        self.cluster_id = cluster_id
        self.user = user
        self.created_on = created_on


@dataclass
class ComputeIdentifier(Identifier):
    name: str

    def get_unique_id(self) -> str:
        return self.name


@dataclass
class ComputeSearchQuery(SearchQuery):
    query: dict

    def get_query(self) -> any:
        return json.loads(json.dumps(self.query))
