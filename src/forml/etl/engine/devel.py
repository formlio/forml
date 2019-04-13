"""
Development ETL engine.
"""
import typing

from forml import etl
from forml.etl import expression
from forml.flow import task


class Source(task.Actor):
    """Custom data-source logic.
    """
    def __init__(self, data: typing.Any):
        super().__init__()
        self._data: typing.Any = data

    @classmethod
    def is_stateful(cls) -> bool:
        return False

    def apply(self) -> typing.Any:  # pylint: disable=arguments-differ
        return self._data

    def set_params(self, **params: typing.Any) -> None:
        return


class Engine(etl.Engine):
    """Development engine.
    """
    def setup(self, select: expression.Select, lower: typing.Optional[etl.OrdinalT],
              upper: typing.Optional[etl.OrdinalT]) -> task.Spec:
        return task.Spec(Source, data=select.data)
