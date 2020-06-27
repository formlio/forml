"""
ETL layer.
"""
import abc
import typing

from forml import provider
from forml.conf import provider as provcfg
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.io.etl import extract

if typing.TYPE_CHECKING:
    from forml.io import etl as etlmod
    from forml.io.dsl import parsing, statement as stmtmod
    from forml.io.dsl.schema import series, frame, kind as kindmod


class Feed(provider.Interface, default=provcfg.Feed.default):
    """ETL feed is the implementation of a specific datasource access layer.
    """
    def __init__(self, **readerkw):
        self._readerkw: typing.Dict[str, typing.Any] = readerkw

    def load(self, source: 'etlmod.Source', lower: typing.Optional['kindmod.Native'] = None,
             upper: typing.Optional['kindmod.Native'] = None) -> pipeline.Segment:
        """Provide a flow track implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Flow track.
        """
        def reader(query: 'stmtmod.Query') -> task.Spec:
            """Helper for creating the reader actor spec for given query.

            Args:
                query: Data loading statement.

            Returns: Reader actor spec.
            """
            return extract.Reader.Actor.spec(self.reader(self.sources, self.columns), extract.Statement.prepare(
                query, source.extract.ordinal, lower, upper), **self._readerkw)

        train: 'stmtmod.Query' = source.extract.train
        label: typing.Optional[task.Spec] = None
        if source.extract.label:
            train = train.select(*(*source.extract.train.columns, *source.extract.label))
            label = extract.Selector.Actor.spec(self.selector(self.columns), source.extract.train.columns,
                                                source.extract.label)
        etl: topology.Composable = extract.Operator(reader(source.extract.apply), reader(train), label)
        if source.transform:
            etl >>= source.transform
        return etl.expand()

    @classmethod
    @abc.abstractmethod
    def reader(cls, sources: typing.Mapping['frame.Source', 'parsing.ResutlT'], columns: typing.Mapping[
            'series.Column', 'parsing.ResutlT']) -> typing.Callable[['stmtmod.Query'], typing.Any]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.

        Returns: Reader instance.
        """

    @classmethod
    def selector(cls, columns: typing.Mapping['series.Column', 'parsing.ResutlT']) -> typing.Callable[
            [typing.Any, typing.Sequence['series.Column']], typing.Any]:
        """Return the selector instance of this feed, that is able to split the loaded dataset column-wise.

        Args:
            columns: Column mappings to be used by the selector.

        Returns: Selector instance.
        """
        raise NotImplementedError(f'No selector implemented for {cls.__name__}')

    @property
    def sources(self) -> typing.Mapping['frame.Source', 'parsing.ResutlT']:
        """The explicit sources mapping implemented by this feed to be used by the query parser.

        Returns: Sources mapping.
        """
        return {}

    @property
    def columns(self) -> typing.Mapping['series.Column', 'parsing.ResutlT']:
        """The explicit columns mapping implemented by this feed to be used by the query parser.

        Returns: Columns mapping.
        """
        return {}
