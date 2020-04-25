"""
Comparison operators and functions.
"""
from forml.etl.expression import symbol


class LessThan(symbol.Bivariate, symbol.Logical):
    """Less-Than operator.
    """


class LessEqual(symbol.Bivariate, symbol.Logical):
    """Less-Equal operator.
    """


class GreaterThan(symbol.Bivariate, symbol.Logical):
    """Greater-Than operator.
    """


class GreaterEqual(symbol.Bivariate, symbol.Logical):
    """Greater-Equal operator.
    """


class Equal(symbol.Bivariate, symbol.Logical):
    """Equal operator.
    """


class NotEqual(symbol.Bivariate, symbol.Logical):
    """Not-Equal operator.
    """


class IsNull(symbol.Bivariate, symbol.Logical):
    """Is-Null operator.
    """


class NotNull(symbol.Bivariate, symbol.Logical):
    """Not-Null operator.
    """
