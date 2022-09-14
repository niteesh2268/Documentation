from abc import abstractmethod
from typing_extensions import Literal
CREATE_ACTION = Literal["CREATE_NEW_VERSION", "OVERWRITE", "NO_ACTION", "CREATE_NEW_VERSION"]


class VersionableEntity:

    @abstractmethod
    def get_create_action(self, other) -> CREATE_ACTION:
        pass
