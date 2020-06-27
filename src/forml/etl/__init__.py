"""
ETL layer.
"""
import abc
import collections
import typing

from forml import provider
from forml.conf import provider as provcfg
from forml.etl import extract
from forml.etl.dsl import parsing, statement as stmtmod
from forml.etl.dsl.schema import kind as kindmod, frame, series
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.project import product


class Field(collections.namedtuple('Field', 'kind, name')):
    """Schema field class.
    """
    def __new__(cls, kind: kindmod.Data, name: typing.Optional[str] = None):
        return super().__new__(cls, kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table schema definitions. Note the meta class is actually going to turn it into an instance
    of frame.Table.
    """


class Source(typing.NamedTuple):
    """Feed independent data provider description.
    """
    extract: 'Source.Extract'
    transform: typing.Optional[topology.Composable] = None

    class Extract(collections.namedtuple('Extract', 'train, apply, label, ordinal')):
        """Combo of select statements for the different modes.
        """
        def __new__(cls, train: stmtmod.Query, apply: stmtmod.Query, label: typing.Sequence[series.Column],
                    ordinal: typing.Optional[series.Element]):
            if {c.element for c in train.columns}.intersection(c.element for c in label):
                raise ValueError('Label-feature overlap')
            if ordinal:
                series.Element.ensure(ordinal)
            return super().__new__(cls, train, apply, tuple(label), ordinal)

    @classmethod
    def query(cls, features: stmtmod.Query, *label: series.Column, apply: typing.Optional[stmtmod.Query] = None,
              ordinal: typing.Optional[series.Element] = None) -> 'Source':
        """Create new source with the given extraction.

        Args:
            features: Query defining the train (and possibly apply) features.
            label: List of training label columns.
            apply: Optional query defining the apply features (if different from train ones).
            ordinal: Optional specification of an ordinal column.

        Returns: New source instance.
        """
        return cls(cls.Extract(features, apply or features, label, ordinal))  # pylint: disable=no-member

    def __rshift__(self, transform: topology.Composable) -> 'Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, topology.Composable], **modules: typing.Any) -> 'product.Artifact':
        """Create an artifact from this source and given pipeline.

        Args:
            pipeline: Pipeline to create the artifact with.
            **modules: Other optional artifact modules.

        Returns: Project artifact instance.
        """
        return product.Artifact(source=self, pipeline=pipeline, **modules)


class Feed(provider.Interface, default=provcfg.Feed.default):
    """ETL feed is the implementation of a specific datasource access layer.
    """
    def __init__(self, **readerkw):
        self._readerkw: typing.Dict[str, typing.Any] = readerkw

    def load(self, source: Source, lower: typing.Optional[kindmod.Native] = None,
             upper: typing.Optional[kindmod.Native] = None) -> pipeline.Segment:
        """Provide a flow track implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Flow track.
        """
        def reader(query: stmtmod.Query) -> task.Spec:
            """Helper for creating the reader actor spec for given query.

            Args:
                query: Data loading statement.

            Returns: Reader actor spec.
            """
            return extract.Reader.Actor.spec(self.reader(self.sources, self.columns), extract.Statement.prepare(
                query, source.extract.ordinal, lower, upper), **self._readerkw)

        train: stmtmod.Query = source.extract.train
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
    def reader(cls, sources: typing.Mapping[frame.Source, parsing.ResultT], columns: typing.Mapping[
            series.Column, parsing.ResultT]) -> typing.Callable[[stmtmod.Query], typing.Any]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.

        Returns: Reader instance.
        """

    @classmethod
    def selector(cls, columns: typing.Mapping[series.Column, parsing.ResultT]) -> typing.Callable[
            [typing.Any, typing.Sequence[series.Column]], typing.Any]:
        """Return the selector instance of this feed, that is able to split the loaded dataset column-wise.

        Args:
            columns: Column mappings to be used by the selector.

        Returns: Selector instance.
        """
        raise NotImplementedError(f'No selector implemented for {cls.__name__}')

    @property
    def sources(self) -> typing.Mapping[frame.Source, parsing.ResultT]:
        """The explicit sources mapping implemented by this feed to be used by the query parser.

        Returns: Sources mapping.
        """
        return {}

    @property
    def columns(self) -> typing.Mapping[series.Column, parsing.ResultT]:
        """The explicit columns mapping implemented by this feed to be used by the query parser.

        Returns: Columns mapping.
        """
        return {}
