"""
Customized dsl errors.
"""
from forml import error


class Mapping(error.Missing):
    """Source/Column mapping exception.
    """


class Unsupported(error.Missing):
    """Indicating feature unsupported by certain parser.
    """
