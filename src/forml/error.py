"""
Core ForML exceptions.
"""


class Error(Exception):
    """Base ForML exception type.
    """


class Invalid(Error):
    """Base invalid state exception.
    """


class Missing(Invalid):
    """Exception state of a missing element.
    """


class Unexpected(Invalid):
    """Exception state of an unexpected element.
    """


class Failed(Error):
    """Exception indicating an unsuccessful result of an operation.
    """
