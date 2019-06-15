"""
ETL layer.
"""
import abc
import collections
import typing

from forml import provider, conf, project
from forml.etl import expression
from forml.flow import task, pipeline
from forml.flow.pipeline import topology
from forml.stdlib import operator

OrdinalT = typing.TypeVar('OrdinalT')


class Extract(collections.namedtuple('Extract', 'train, apply')):
    """Combo of select statements for the different modes.
    """
    def __new__(cls, train: expression.Select, apply: typing.Optional[expression.Select] = None):
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

    def bind(self, pipeline: typing.Union[str, topology.Composable], **modules: typing.Any) -> 'project.Artifact':
        """Create an artifact from this source and given pipeline.

        Args:
            pipeline: Pipeline to create the artifact with.
            **modules: Other optional artifact modules.

        Returns: Project artifact instance.
        """
        return project.Artifact(source=self, pipeline=pipeline, **modules)


class Engine(provider.Interface, default=conf.ENGINE):
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
    def setup(self, select: expression.Select,
              lower: typing.Optional[OrdinalT], upper: typing.Optional[OrdinalT]) -> task.Spec:
        """Actual engine provider to be implemented by subclass.

        Args:
            select: The select statement.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Actor task spec.
        """
