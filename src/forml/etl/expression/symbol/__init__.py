"""
Expression symbols.
"""
import abc

from forml.etl import schema, kind as kindmod


class Bivariate(schema.Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands.
    """
    def __new__(cls, arg1: schema.Column, arg2: schema.Column):
        return super().__new__(cls, arg1, arg2)


class Logical:
    """Mixin for logical functions/operators.
    """
    kind = kindmod.Boolean()
