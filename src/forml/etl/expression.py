"""
ETL expression language.
"""


class Select:
    """ForML ETL select statement.
    """
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__class__ is other.__class__
