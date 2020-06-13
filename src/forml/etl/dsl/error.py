"""
Customized dsl errors.
"""
from forml import error


class Mapping(error.Missing):
    """Source/Column mapping exception.
    """
