"""
ETL layer.
"""
import abc
import collections
import typing

from forml import provider, error
from forml.conf import provider as provcfg
from forml.etl import extract
from forml.etl.dsl import parsing, statement as stmntmod
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
    """Engine independent data provider description.
    """
    extract: 'Source.Extract'
    transform: typing.Optional[topology.Composable] = None

    class Extract(typing.NamedTuple):
        """Combo of select statements for the different modes.
        """
        train: 'extract.Statement'
        apply: 'extract.Statement'

    @classmethod
    @typing.overload
    def query(cls, train: stmntmod.Query, apply: typing.Optional[stmntmod.Query] = None,
              ordinal: typing.Optional[series.Column] = None) -> 'Source':
        """Query signature with plain train/apply queries and common ordinal expression.

        Args:
            train: Train query.
            apply: Optional apply query.
            ordinal: Optional ordinal expression common to both train and apply queries.

        Returns: Source instance.
        """

    @classmethod
    @typing.overload
    def query(cls, train: typing.Tuple[stmntmod.Query, series.Column],
              apply: typing.Optional[typing.Tuple[stmntmod.Query, series.Column]] = None) -> 'Source':
        """Query signature with train/apply specs provided with specific distinct ordinal expression each.

        Args:
            train: Tuple of train query and ordinal expression.
            apply: Optional tuple of apply query and ordinal expression.

        Returns: Source instance.
        """

    @classmethod
    def query(cls, *, train, apply=None, ordinal=None):
        """Actual implementations of the overloaded versions of query - see above for details.
        """
        if isinstance(train, stmntmod.Query):  # plain query with (optional) common ordinal expression
            train = extract.Statement(train, ordinal)
            if apply:
                if not isinstance(apply, stmntmod.Query):
                    raise error.Missing('Plain apply query expected')
                apply = extract.Statement(apply, ordinal)
        else:  # explicit per query ordinal expression
            if ordinal is not None:
                raise error.Unexpected('Common ordinal not expected')
            train = extract.Statement(*train)
            if apply:
                if isinstance(apply, stmntmod.Query):
                    raise error.Missing('Ordinal for apply query required')
                apply = extract.Statement(*apply)
        return cls(cls.Extract(train, apply or train))  # pylint: disable=no-member

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


class Engine(provider.Interface, default=provcfg.Engine.default):
    """ETL engine is the implementation of a specific datasource access layer.
    """
    class Reader(extract.Reader, metaclass=abc.ABCMeta):
        """Engine specific reader implementation
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
        apply: task.Spec = self.setup(source.extract.apply.bind(lower, upper))
        train: task.Spec = self.setup(source.extract.train.bind(lower, upper))
        etl: topology.Composable = extract.Operator(apply, train)
        if source.transform:
            etl >>= source.transform
        return etl.expand()

    def setup(self, statement: extract.Statement.Binding) -> task.Spec:  # pylint: disable=no-member
        """Actual engine provider to be implemented by subclass.

        Args:
            statement: The select statement binding.

        Returns: Actor task spec.
        """
        return extract.Actor.spec(self.Reader(  # pylint: disable=abstract-class-instantiated
            self.sources, self.columns), statement, **self._readerkw)

    @property
    @abc.abstractmethod
    def sources(self) -> typing.Mapping[frame.Source, parsing.ResultT]:
        """The explicit sources mapping implemented by this engine to be used by the query parser.

        Returns: Sources mapping.
        """
        return {}

    @property
    def columns(self) -> typing.Mapping[series.Column, parsing.ResultT]:
        """The explicit columns mapping implemented by this engine to be used by the query parser.

        Returns: Columns mapping.
        """
        return {}
