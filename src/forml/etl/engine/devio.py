"""
Development ETL engine.
"""
import typing

from forml import etl
from forml.etl import extract
from forml.etl.dsl import parsing, statement as stmntmod
from forml.etl.dsl.schema import kind, frame, series
from forml.flow import task


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
    def setup(self, statement: 'extract.Statement.Binding') -> task.Spec:
        params = dict()
        if statement.lower:
            params['lower'] = statement.lower
        if statement.upper:
            params['upper'] = statement.upper
        return Source.spec(producer=statement.producer, **params)

    @classmethod
    def reader(cls, sources: typing.Mapping[frame.Source, parsing.ResultT], columns: typing.Mapping[
            series.Column, parsing.ResultT]) -> typing.Callable[[stmntmod.Query], typing.Any]:
        pass
