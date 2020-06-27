"""
Development ETL feed.
"""
import typing

from forml import io
from forml.io.etl import extract
from forml.io.dsl import parsing
from forml.io.dsl import statement as stmntmod
from forml.io.dsl.schema import series, frame, kind
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


class Feed(io.Feed, key='devio'):
    """Development feed.
    """
    def setup(self, statement: 'extract.Statement.Prepared') -> task.Spec:
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
