from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin
from typeguard import typechecked
from mlp_commons.models.identifier import Identifier


@dataclass
@typechecked()
class Disk:
    type: str
    size: int

    def __init__(self, disk_type: str, disk_size: int):
        self.type = disk_type
        self.size = disk_size


@dataclass
class WorkerTemplate(DataClassJsonMixin):
    template_id: str
    display_name: str
    memory_per_core: int

    def __init__(self, template_id, display_name, memory_per_core):
        self.template_id = template_id
        self.display_name = display_name
        self.memory_per_core = memory_per_core


@dataclass
class InstanceRole(DataClassJsonMixin):
    instance_role_id: str
    display_name: str

    def __init__(self, instance_role_id, display_name):
        self.instance_role_id = instance_role_id
        self.display_name = display_name


@dataclass
class Azs(DataClassJsonMixin):
    az_id: str
    display_name: str

    def __int__(self, az_id, display_name):
        self.az_id = az_id
        self.display_name = display_name


@dataclass
class ClusterIdentifier(Identifier):
    name: str

    def get_unique_id(self) -> str:
        return self.name
