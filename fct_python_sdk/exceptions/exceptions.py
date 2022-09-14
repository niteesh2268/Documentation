"""
Exceptions thrown by the fct fct_python_sdk
"""


class FeatureGroupNotFound(Exception):
    """This exception will be raised if a requested featuregroup cannot be found"""


class FeatureNotFound(Exception):
    """This exception will be raised if a requested identifier cannot be found"""


class FeatureNameCollisionError(Exception):
    """This exception will be raised if a requested identifier cannot be uniquely identified in the identifier store"""


class InvalidRequestParam(Exception):
    """This exception will be raised if a request is invalid"""


class InvalidEnvironment(Exception):
    """This exception will be raised if the environment provided is not valid"""
