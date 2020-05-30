"""
ETL layer.
"""
import abc
import collections
import typing

from forml import provider
from forml.conf import provider as provcfg
from forml.etl.dsl import statement
from forml.etl.dsl.schema import kind as kindmod, frame, series
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.project import product
from forml.stdlib import operator as oplib


class Field(collections.namedtuple('Field', 'kind, name')):
    """Schema field class.
    """
    def __new__(cls, kind: kindmod.Data, name: typing.Optional[str] = None):
        return super().__new__(cls, kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table schema definitions. Note the meta class is actually going to turn it into an instance
    of schema.Table.
    """


class Source(collections.namedtuple('Source', 'extract, transform')):
    """Engine independent data provider description.
    """
    class Extract(collections.namedtuple('Extract', 'train, apply')):
        """Combo of select statements for the different modes.
        """
        class Select(collections.namedtuple('Select', 'query, ordinal')):
            """Select statement defined as a query and definition of the ordinal expression.
            """
            def __new__(cls, query: statement.Query, ordinal: typing.Optional[series.Column]):
                return super().__new__(cls, query, ordinal)

            def __call__(self, lower: typing.Optional[kindmod.Native] = None,
                         upper: typing.Optional[kindmod.Native] = None):
                query = self.query
                if self.ordinal is not None:
                    if lower:
                        query = query.where(self.ordinal >= lower)
                    if upper:
                        query = query.where(self.ordinal < upper)
                elif lower or upper:
                    raise TypeError('Bounds provided but source not ordinal')
                return query

        def __new__(cls, train: Select, apply: Select):
            return super().__new__(cls, train, apply)

    def __new__(cls, extract: Extract, transform: typing.Optional[topology.Composable] = None):
        return super().__new__(cls, extract, transform)

    @classmethod
    @typing.overload
    def query(cls, train: statement.Query, apply: typing.Optional[statement.Query] = None,
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
    def query(cls, train: typing.Tuple[statement.Query, series.Column],
              apply: typing.Optional[typing.Tuple[statement.Query, series.Column]] = None) -> 'Source':
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
        if isinstance(train, statement.Query):  # plain query with (optional) common ordinal expression
            train = cls.Extract.Select(train, ordinal)
            if apply:
                if not isinstance(apply, statement.Query):
                    raise TypeError('Plain apply query expected')
                apply = cls.Extract.Select(apply, ordinal)
        else:  # explicit per query ordinal expression
            if ordinal is not None:
                raise TypeError('Common ordinal not expected')
            train = cls.Extract.Select(*train)
            if apply:
                if isinstance(apply, statement.Query):
                    raise TypeError('Ordinal for apply query required')
                apply = cls.Extract.Select(*apply)
        return cls(cls.Extract(train, apply or train))

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
    def load(self, source: Source, lower: typing.Optional[kindmod.Native] = None,
             upper: typing.Optional[kindmod.Native] = None) -> pipeline.Segment:
        """Provide a flow track implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Flow track.
        """
        apply: task.Spec = self.setup(source.extract.apply, lower, upper)
        train: task.Spec = self.setup(source.extract.train, lower, upper)
        etl: oplib.Loader = oplib.Loader(apply, train)
        if source.transform:
            etl >>= source.transform
        return etl.expand()

    @abc.abstractmethod
    def setup(self, select: Source.Extract.Select,
              lower: typing.Optional[kindmod.Native], upper: typing.Optional[kindmod.Native]) -> task.Spec:
        """Actual engine provider to be implemented by subclass.

        Args:
            select: The select statement.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Actor task spec.
        """
