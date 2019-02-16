import abc
import collections
import typing

from forml.flow import segment, task, operator


SelectT = typing.TypeVar('SelectT')
OrdinalT = typing.TypeVar('OrdinalT')


class Extract(typing.Generic[SelectT], collections.namedtuple('Extract', 'apply, train')):
    """Combo of select statements for the different modes.
    """
    def __new__(cls, apply: SelectT, train: typing.Optional[SelectT] = None):
        return super().__new__(cls, apply, train or apply)


class Source(typing.Generic[SelectT], collections.namedtuple('Source', 'extract, transform')):
    """Engine independent data provider description.
    """
    def __new__(cls, extract: Extract[SelectT], transform: typing.Optional[segment.Expression] = None):
        return super().__new__(cls, extract, transform)

    def __rshift__(self, transform: 'segment.Composable') -> 'Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)


class Engine(typing.Generic[SelectT, OrdinalT], metaclass=abc.ABCMeta):
    """ETL engine is the implementation of a specific datasource access layer.
    """
    def load(self, source: Source, lower: typing.Optional[OrdinalT] = None,
             upper: typing.Optional[OrdinalT] = None) -> segment.Track:
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
        return etl.track()

    @abc.abstractmethod
    def setup(self, select: SelectT, lower: typing.Optional[OrdinalT], upper: typing.Optional[OrdinalT]) -> task.Spec:
        """Actual engine provider to be implemented by subclass.

        Args:
            select: The select statement.
            lower: Optional ordinal lower bound.
            upper: Optional ordinal upper bound.

        Returns: Actor task spec.
        """
