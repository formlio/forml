"""
Flow segments represent partial pipeline blocks during pipeline assembly.
"""
import abc
import weakref

from forml.flow import pipeline, graph


class Composable(metaclass=abc.ABCMeta):
    """Common base for operators and expressions.
    """
    @abc.abstractmethod
    def expand(self) -> pipeline.Segment:
        """Compose and return a segment track.

        Returns: Segment track.
        """

    def __rshift__(self, right: 'Composable') -> 'Compound':
        """Semantical composition construct.
        """
        return Compound(right, self)

    def __str__(self):
        return self.__class__.__name__

    @abc.abstractmethod
    def compose(self, left: 'Composable') -> pipeline.Segment:
        """Expand the left segment producing new composed segment track.

        Args:
            left: Left side composable.

        Returns: Composed segment track.
        """


class Origin(Composable):
    """Initial builder without a predecessor.
    """
    def expand(self) -> pipeline.Segment:
        """Track of future nodes.

        Returns: Segment track.
        """
        return pipeline.Segment()

    def compose(self, left: Composable) -> pipeline.Segment:
        """Origin composition is just the left side track.

        Args:
            left: Left side composable.

        Returns: Segment track.
        """
        return left.expand()


class Operator(Composable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base pipeline entity.
    """
    def expand(self) -> pipeline.Segment:
        """Create dummy composition of this operator on a future origin nodes.

        Returns: Segment track.
        """
        return self.compose(Origin())


class Compound(Composable):
    """Operator chaining descriptor.
    """
    _TERMS = weakref.WeakValueDictionary()

    def __init__(self, right: Composable, left: Composable):
        for term in (left, right):
            if not isinstance(term, Composable):
                raise ValueError(f'{type(term)} not composable')
            if term in self._TERMS:
                raise graph.Error(f'Non-linear {term} composition')
            self._TERMS[term] = self
        self._right: Composable = right
        self._left: Composable = left

    def __str__(self):
        return f'{self._left} >> {self._right}'

    def expand(self) -> pipeline.Segment:
        """Compose the segment track.

        Returns: Segment track.
        """
        return self._right.compose(self._left)

    def compose(self, left: Composable) -> pipeline.Segment:
        """Expression composition is just extension of its tracks.

        Args:
            left: Left side composable.

        Returns: Segment track.
        """
        return left.expand().extend(*self.expand())
