from dataclasses import *

from dataclasses_json import DataClassJsonMixin
from typeguard import typechecked


@dataclass
@typechecked
class AdvanceConfig(DataClassJsonMixin):
    """
       Advance configuration for compute request.

       Args:
            env_variables: ,
            log_path: ,
            init_script: ,
            instance_role: ,
            availability_zone:
       """
    env_variables: str
    log_path: str
    init_script: str
    instance_role: str
    availability_zone: str

    def __init__(
            self,
            env_variables: str,
            log_path: str,
            init_script: str,
            instance_role: str,
            availability_zone: str
    ):
        """Creates a AdvanceConfig object."""

        if not isinstance(env_variables, str):
            raise ValueError("env_variables = %s is not a valid input" % env_variables)
        self.env_variables = env_variables
        if not isinstance(log_path, str):
            raise ValueError("log_path = %s is not a valid input" % log_path)
        self.log_path = log_path
        if not isinstance(init_script, str):
            raise ValueError("init_script = %s is not a valid input" % init_script)
        self.init_script = init_script
        if not isinstance(instance_role, str):
            raise ValueError("instance_role = %s is not a valid input", instance_role)
        self.instance_role = instance_role
        if not isinstance(availability_zone, str):
            raise ValueError("availability_zone = %s is not a valid input" % availability_zone)
        self.availability_zone = availability_zone
