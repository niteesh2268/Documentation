from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin
from typeguard import typechecked


@typechecked
@dataclass
class HeadNode(DataClassJsonMixin):
    head_node_cores: int
    head_node_memory: int

    def __init__(
            self,
            head_node_cores: int,
            head_node_memory: int
    ):
        self.head_node_cores = head_node_cores
        self.head_node_memory = head_node_memory
