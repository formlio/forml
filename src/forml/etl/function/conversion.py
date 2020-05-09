"""
Conversion functions.
"""
import operator

from forml.etl.schema import series, kind as kindmod


class Cast(series.Expression):
    """Explicitly cast value as given kind.
    """
    value: series.Column = property(operator.itemgetter(0))
    kind: kindmod.Data = property(operator.itemgetter(1))

    def __new__(cls, value: series.Column, kind: kindmod.Data):
        return super().__new__(cls, value, kind)
