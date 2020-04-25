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
