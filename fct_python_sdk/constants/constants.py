from typing_extensions import Literal

ENV_TYPE = Literal["prod", "stag", "test", "local"]


class CREATE_ACTIONS:
    def __init__(self):
        pass

    CREATE_NEW = "CREATE_NEW"
    OVERWRITE = "OVERWRITE"
    NO_ACTION = "NO_ACTION"
    CREATE_NEW_VERSION = "CREATE_NEW_VERSION"


class RESPONSE:
    def __init__(self):
        pass

    SUCCESS_RESPONSE = {
        "status": "Success",
        "create_action": "",
        "feature_group_details": None
    }
