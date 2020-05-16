"""
ETL layer.
"""
import abc
import collections
import typing

from forml import provider
from forml.conf import provider as provcfg
from forml.etl.dsl import statement
from forml.etl.dsl.schema import kind as kindmod, frame
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.project import product
from forml.stdlib import operator

OrdinalT = typing.TypeVar('OrdinalT')


class Field(collections.namedtuple('Field', 'kind, name')):
    """Schema field class.
    """
    def __new__(cls, kind: kindmod.Data, name: typing.Optional[str] = None):
        return super().__new__(cls, kind, name)


class Schema(metaclass=frame.Table):  # pylint: disable=invalid-metaclass
    """Base class for table schema definitions. Note the meta class is actually going to turn it into an instance
    of schema.Table.
    """


class Extract(collections.namedtuple('Extract', 'train, apply')):
    """Combo of select statements for the different modes.
    """
    def __new__(cls, train: statement.Query, apply: typing.Optional[statement.Query] = None):
        return super().__new__(cls, train, apply or train)

    def __rshift__(self, transform: topology.Composable) -> 'Source':
        return Source(self, transform)


class Source(collections.namedtuple('Source', 'extract, transform')):
    """Engine independent data provider description.
    """
    def __new__(cls, extract: Extract, transform: typing.Optional[topology.Composable] = None):
        return super().__new__(cls, extract, transform)

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
    def load(self, source: Source, lower: typing.Optional[OrdinalT] = None,
             upper: typing.Optional[OrdinalT] = None) -> pipeline.Segment:
        """Provide a flow track implementing the etl actions.

        Args:
            source: Independent datasource descriptor.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Flow track.
        """
        apply: task.Spec = self.setup(source.extract.apply, lower, upper)
        train: task.Spec = self.setup(source.extract.train, lower, upper)
        etl: operator.Loader = operator.Loader(apply, train)
        if source.transform:
            etl >>= source.transform
        return etl.expand()

    @abc.abstractmethod
    def setup(self, select: statement.Query,
              lower: typing.Optional[OrdinalT], upper: typing.Optional[OrdinalT]) -> task.Spec:
        """Actual engine provider to be implemented by subclass.

        Args:
            select: The select statement.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Actor task spec.
        """
