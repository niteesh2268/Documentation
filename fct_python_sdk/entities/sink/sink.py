from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin
from typing_extensions import Literal

from fct_python_sdk.entities.feature_metada import CREATE_ACTION, VersionableEntity


@dataclass
class Sink(DataClassJsonMixin, VersionableEntity):
    location: str
    type: Literal["s3", "redshift"]

    def __init__(self, location: str, type: Literal["s3", "redshift"]):
        self.location = location
        self.type = type

    def __hash__(self):
        return hash(tuple(self))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, Sink):
            raise TypeError("Comparisons should only involve Sink class objects. Please check the object types")

        if (
                self.location != other.location
        ):
            return False

        return True
    def __lt__(self, other):
        return self.location < other.location

    def get_create_action(self, other) -> CREATE_ACTION:
        if not isinstance(other, Sink):
            raise TypeError("Comparisons should only involve Feature class objects. Please check the object types")

        if self.location != other.location or self.type != other.type:
            return "CREATE_NEW_VERSION"
        else:
            return "NO_ACTION"
