from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin
from fct_python_sdk.entities.feature_metada import CREATE_ACTION, VersionableEntity


@dataclass
class ComputeMetaData(DataClassJsonMixin, VersionableEntity):
    compute_type: str
    link: str


    def __init__(self, compute_type: str, link: str):
        self.compute_type = compute_type
        self.link = link

    def __hash__(self):
        return hash(tuple(self))

    def __eq__(self, other) -> bool:
        if other is None:
            return False

        if not isinstance(other, ComputeMetaData):
            raise TypeError("Comparisons should only involve Source class objects. Please check the object types")

        if (
                self.compute_type != other.compute_type or
                self.link != other.link
        ):
            return False

        return True

    def __lt__(self, other):
        return self.location < other.location

    def get_create_action(self, other) -> CREATE_ACTION:
        if not isinstance(other, ComputeMetaData):
            raise TypeError("Comparisons should only involve Compute Metadata class objects. Please check the object types")

        if self.compute_type != other.compute_type or self.link != other.link:
            return "CREATE_NEW_VERSION"
        else:
            return "NO_ACTION"
