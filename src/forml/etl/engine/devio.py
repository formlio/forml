"""
Development ETL engine.
"""
import typing

from forml import etl
from forml.flow import task
from forml.etl.dsl.schema import kind


class Source(task.Actor):
    """Custom data-source logic.
    """
    def __init__(self,
                 producer: typing.Callable[[typing.Optional[kind.Native], typing.Optional[kind.Native]], typing.Any],
                 **params):
        super().__init__()
        self._producer: typing.Callable[[typing.Optional[kind.Native],
                                         typing.Optional[kind.Native]], typing.Any] = producer
        self._params = params

    def apply(self) -> typing.Any:  # pylint: disable=arguments-differ
        return self._producer(**self._params)


class Engine(etl.Engine, key='devio'):
    """Development engine.
    """
    def setup(self, select: 'etl.Source.Extract.Statement.Binding') -> task.Spec:
        params = dict(select.params)
        if select.lower:
            params['lower'] = select.lower
        if select.upper:
            params['upper'] = select.upper
        return Source.spec(producer=select.producer, **params)
