"""
ETL expression language.
"""
import typing


class Select:
    """ForML ETL select statement.

    This is just a hacked implementation as the the ETL engine concept needs yet to be developed.
    """
    def __init__(self, data: typing.Any = None):
        self.data: typing.Any = data

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__class__ is other.__class__
