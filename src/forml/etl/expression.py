"""
ETL expression language.
"""
import typing


class Select:
    """ForML ETL select statement.

    This is just a hacked implementation as the the ETL engine concept needs yet to be developed.
    """
    def __init__(self, producer: typing.Callable = None, **params):
        self.producer: typing.Callable = producer
        self.params = params

    def __eq__(self, other):
        return self.__class__ is other.__class__

    def __str__(self):
        return self.__class__.__name__
