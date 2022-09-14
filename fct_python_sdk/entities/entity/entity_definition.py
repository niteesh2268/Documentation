from dataclasses import dataclass
from typing import List

from mlp_commons.models.identifier import Identifier
from mlp_commons.models.meta_data import MetaData
from typeguard import typechecked

@dataclass
@typechecked
class EntityDefinition(MetaData):
    """
    Entity definition class
    """
    name: str
    value_type: str
    join_keys: List[str]
    description: str

    def __init__(self, name: str, value_type: str, join_keys: List[str], description: str):
        self.name = name
        self.value_type = value_type
        self.join_keys = join_keys
        self.description = description

@typechecked
@dataclass
class EntityIdentifier(Identifier):
    name: str

    def get_unique_id(self):
        return self.name
