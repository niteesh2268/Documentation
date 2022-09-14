from dataclasses import *

from dataclasses_json import DataClassJsonMixin
from typeguard import typechecked


@dataclass
@typechecked()
class Disk:
    disk_type: str
    disk_size: int

    def __init__(
            self,
            disk_type: str,
            disk_size: int
    ):
        if not isinstance(disk_type, str):
            raise ValueError("disk_type = %s is not a valid input" % disk_type)
        self.disk_type = disk_type
        if not isinstance(disk_size, int) or disk_size <= 0:
            raise ValueError("disk_size %s is not an valid input" % disk_size)
        self.disk_size = disk_size


@dataclass
@typechecked
class WorkerGroup(DataClassJsonMixin):
    """
       Worker group configuration for compute request.

       Args:
            cores: Number of cores per worker,
            memory: Memory in GBs per worker,
            min_pods: Minimum number of pods,
            max_pods: Maximum number of pods,
            disk: Disk type of worker
       """
    cores: int
    memory: int
    min_pods: int
    max_pods: int
    disk: Disk

    def __init__(
            self,
            cores: int,
            memory: int,
            min_pods: int,
            max_pods: int,
            disk: Disk
    ):
        if not isinstance(cores, int) or cores <= 0:
            raise ValueError("cores = %s is not a valid input" % cores)
        self.cores = cores
        if not isinstance(memory, int) or memory <= 0:
            raise ValueError("memory = %s is not a valid input" % memory)
        self.memory = memory
        if not isinstance(min_pods, int) or min_pods < 0:
            raise ValueError("min_pods = %s is not a valid input" % min_pods)
        self.min_pods = min_pods
        if not isinstance(max_pods, int) or max_pods <= 0:
            raise ValueError("max_pods = %s is not a valid input", max_pods)
        self.max_pods = max_pods
        if not isinstance(disk, Disk):
            raise ValueError("disk is not a valid input")
        self.disk = disk
