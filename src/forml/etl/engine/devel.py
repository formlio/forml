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
    def __init__(self,
                 producer: typing.Callable[[typing.Optional[etl.OrdinalT], typing.Optional[etl.OrdinalT]], typing.Any],
                 lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT]):
        super().__init__()
        self._producer: typing.Callable[[typing.Optional[etl.OrdinalT],
                                         typing.Optional[etl.OrdinalT]], typing.Any] = producer
        self._lower: typing.Optional[etl.OrdinalT] = lower
        self._upper: typing.Optional[etl.OrdinalT] = upper

    def apply(self) -> typing.Any:  # pylint: disable=arguments-differ
        return self._producer(self._lower, self._upper)


class Engine(etl.Engine):
    """Development engine.
    """
    def setup(self, select: expression.Select, lower: typing.Optional[etl.OrdinalT],
              upper: typing.Optional[etl.OrdinalT]) -> task.Spec:
        return task.Spec(Source, producer=select.producer, lower=lower, upper=upper)
