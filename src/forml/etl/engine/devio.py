"""
Development ETL engine.
"""
import typing

from forml import etl
from forml.etl.dsl import function
from forml.flow import task


class Source(task.Actor):
    """Custom data-source logic.
    """
    def __init__(self,
                 producer: typing.Callable[[typing.Optional[etl.OrdinalT], typing.Optional[etl.OrdinalT]], typing.Any],
                 **params):
        super().__init__()
        self._producer: typing.Callable[[typing.Optional[etl.OrdinalT],
                                         typing.Optional[etl.OrdinalT]], typing.Any] = producer
        self._params = params

    def apply(self) -> typing.Any:  # pylint: disable=arguments-differ
        return self._producer(**self._params)


class Engine(etl.Engine, key='devio'):
    """Development engine.
    """
    def setup(self, select: function.Select, lower: typing.Optional[etl.OrdinalT],
              upper: typing.Optional[etl.OrdinalT]) -> task.Spec:
        params = dict(select.params)
        if lower:
            params['lower'] = lower
        if upper:
            params['upper'] = upper
        return Source.spec(producer=select.producer, **params)
