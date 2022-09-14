from dataclasses import dataclass
from typing import List

from dataclasses_json import DataClassJsonMixin

from fct_python_sdk.entities.feature_metada import VersionableEntity, CREATE_ACTION


@dataclass
class Entity(DataClassJsonMixin, VersionableEntity):
    name: str
    value_type: str
    join_type: List[str]
    description: str

    def __int__(self, name: str, value_type : str, join_type: List[str], description: str):
        self.name = name
        self.value_type = value_type
        self.description = description
        self.join_type = join_type

    def __eq__(self, other):
        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity objects. Please check the object types")

        if self.name != other.name or self.value_type != other.value_type or self.description != other.description or \
                self.join_type != other.join_type:
            return False
        return True

    def __lt__(self, other):

        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity objects. Please check the object types")

        if self.name < other.name:
            return True
        elif self.name == other.name and self.value_type < other.value_type:
            return True
        elif self.name == other.name and self.value_type == other.value_type and self.description < other.description:
            return True
        elif self.name == other.name and self.value_type == other.value_type and self.description == other.description and self.join_type.sort() < other.join_type.sort():
            return True
        else:
            return False

    def get_create_action(self, other) -> CREATE_ACTION:
        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity objects. Please check the object types")

        if self.name != other.name or self.type != other.type:
            return "CREATE_NEW_VERSION"
        else:
            return "NO_ACTION"








