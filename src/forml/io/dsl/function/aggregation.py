"""
Aggregation functions.
"""

from forml.io.dsl.schema import series, kind as kindmod


class Count(series.Univariate):
    """Get the number of input rows.
    """
    kind: kindmod.Integer = kindmod.Integer()
