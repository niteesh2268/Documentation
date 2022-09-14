from dataclasses import dataclass
from typing_extensions import Literal
from typing import TypeVar, Generic, Optional

T = TypeVar('T')
ACTION = Literal["CREATE", "READ", "UPDATE", 'SEARCH']
STATUS = Literal["SUCCESS", "ERROR"]


@dataclass
class FCTResponse(Generic[T]):
    """Response given by the FCT."""

    action: ACTION
    status: STATUS
    message: str
    data: T

    def __init__(self, action: ACTION, status: STATUS, message: str, data: T):
        self.action = action
        self.status = status
        self.message = message
        self.data = data

    @classmethod
    def success_response(cls, action: ACTION, data: T):
        """Class Constructor for success response"""
        return cls(action, "SUCCESS", "", data)
        return {"action": action, "status": "SUCCESS", "data": data}

    @classmethod
    def success_response_with_message(cls, action: ACTION, data: T, message: str):
        """Class Constructor for success response with message"""
        return {"action": action, "status": "SUCCESS", "message": message, "data": data}

    @classmethod
    def error_response(cls, action: ACTION, message: str):
        """Class Constructor for error response"""
        return {"action": action, "status": "ERROR", "message": message}
