"""
Comparison operators and functions.
"""
from forml.etl import kind as kindmod, schema


class LessThan(schema.Expression):
    """Less-Than operator.
    """
    def __new__(cls, left: schema.Column, right: schema.Column):
        return super().__new__(cls, left, right)

    kind = kindmod.Boolean()
