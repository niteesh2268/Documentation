from dataclasses import dataclass
from typing import List
from mlp_commons.models.identifier import Identifier
from mlp_commons.models.meta_data import MetaData
from typing_extensions import Literal

from fct_python_sdk.entities.ComputeMetaData import ComputeMetaData
from fct_python_sdk.entities.feature_metada import VersionableEntity, CREATE_ACTION
from fct_python_sdk.entities.sink.sink import Sink
from fct_python_sdk.entities.feature import Feature
from fct_python_sdk.entities.source.source import Source
from fct_python_sdk.utils.utils import get_create_action, compare_list_if_not_equal


@dataclass
class FeatureGroupDefinition(MetaData, VersionableEntity):
    name: str
    status: Literal["ACTIVE", "INACTIVE"]
    created_at: str
    compute_meta_data: ComputeMetaData
    version: int
    sinks: List[Sink]
    tags: List[str]
    description: str
    entities: List[str]
    user: str
    notification_channel: List[str]
    sources: List[Source]
    schema: List[Feature]

    def __init__(self, name: str,
                 status: Literal["ACTIVE", "INACTIVE"],
                 created_at: str,
                 compute_meta_data: ComputeMetaData,
                 version: int,
                 sinks: List[Sink],
                 tags: List[str],
                 description: str,
                 entities: List[str],
                 user: str,
                 notification_channel: List[str],
                 sources: List[Source],
                 schema: List[Feature]
                 ):
        self.name = name
        self.status = status
        self.created_at = created_at
        self.compute_meta_data = compute_meta_data
        self.description = description
        self.entities = entities
        self.tags = tags
        self.user = user
        self.notification_channel = notification_channel
        self.sinks = sinks
        self.sources = sources
        self.schema = schema
        self.version = version

    def get_create_action(self, other) -> CREATE_ACTION:
        if (get_create_action(self.schema, other.schema) == "CREATE_NEW_VERSION" or get_create_action(self.sources,
                                                                                              other.sources) == "CREATE_NEW_VERSION" or
                compare_list_if_not_equal(self.entities, other.entities) or get_create_action(self.sinks,
                                                                                              other.sinks) == "CREATE_NEW_VERSION"):
            return "CREATE_NEW_VERSION"
        else:
            return "NO_ACTION"

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, FeatureGroupDefinition):
            raise TypeError(
                "Comparisons should only involve FeatureGroupDefinition class objects. Please check the object types")

        if self.name != other.name or self.schema != other.schema or self.sinks != other.sinks or self.sources != other.sources \
                or self.compute_meta_data != other.compute_meta_data:
            return False

        return True


@dataclass
class FgIdentifier(Identifier):
    name: str
    version: str

    def get_unique_id(self):
        return self.name + '__' + self.version
