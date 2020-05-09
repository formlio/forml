"""
Conversion functions.
"""
import operator

from forml.etl import schema, kind as kindmod


class Cast(schema.Expression):
    """Explicitly cast value as given kind.
    """
    value: schema.Column = property(operator.itemgetter(0))
    kind: kindmod.Data = property(operator.itemgetter(1))

    def __new__(cls, value: schema.Column, kind: kindmod.Data):
        return super().__new__(cls, value, kind)
