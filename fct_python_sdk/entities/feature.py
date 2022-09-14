from dataclasses import dataclass
from typing import List

from dataclasses_json import DataClassJsonMixin

from fct_python_sdk.entities.feature_metada import VersionableEntity, CREATE_ACTION


@dataclass
class Feature(DataClassJsonMixin, VersionableEntity):
    name: str
    type: str
    tags: List[str]
    description: str

    def __int__(self, name: str, type : str, tags: List[str], description: str):
        self.name = name
        self.type = type
        self.description = description
        self.tags = tags

    def __eq__(self, other):
        if not isinstance(other, Feature):
            raise TypeError("Comparisons should only involve Feature class1 objects. Please check the object types")

        if self.name != other.name or self.type != other.type or self.description != other.description or \
                self.tags != other.tags:
            return False
        return True

    def __lt__(self, other):
        if self.name < other.name:
            return True
        elif self.name == other.name:
            return self.type < other.type
        else:
            return False

    def get_create_action(self, other) -> CREATE_ACTION:
        if not isinstance(other, Feature):
            raise TypeError("Comparisons should only involve Feature class objects. Please check the object types")

        if self.name != other.name or self.type != other.type:
            return "CREATE_NEW_VERSION"
        else:
            return "NO_ACTION"








