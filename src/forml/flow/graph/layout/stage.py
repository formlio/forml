"""
Graph stage (group of nodes with same distance from root) entities.
"""

import abc
import typing

from forml.flow.graph.layout import node


class Abstract(metaclass=abc.ABCMeta):
    """Group of nodes representing same stage of the flow with non-commutative abstract connection operations that
    the child classes implement depending on specific order of operations.
    """
    def __init__(self, apply, train):
        self._nodes: typing.Sequence[node.Primitive] = nodes

    def __len__(self):
        return len(self._nodes)

    def __iter__(self):
        return iter(self._nodes)

    @abc.abstractmethod
    @property
    def shape(self) -> typing.Sequence[int]:
        """Cardinality of the apply ports (numbers of individual apply ports - input or output depending of direction).

        Returns: Cardinality of apply ports.
        """


class Tail(Abstract):
    """Group of nodes representing output stage that can publish into some other Head stage(s).
    """

    @property
    def shape(self) -> typing.Sequence[int]:
        """Shapes of output apply ports (numbers of individual apply outputs).

        Returns: Shapes of output apply ports.
        """
        return tuple(n.szout for n in self)

    def apply(self, right: 'Head') -> 'Tail':
        """Connect data (apply) output of this stage with data (apply) input of the right stage.

        Args:
            right: Other stage to which input our output should be connected.

        Returns: right stage
        """
        assert sum(self.shape) == sum(right.shape), 'Incompatible stages'
        for src, (dst, idx) in zip((o for n in self for o in n.apply), ((n, i) for n in right for i in range(n.szin))):
            src.apply(dst, idx)
        return Tail(*right)

    def train(self, right: 'Head') -> 'Tail':
        """Connect apply output of this stage to train input of the right stage.

        Args:
            right: Other stage to which train input our apply output should be connected.

        Returns: right stage.
        """
        assert sum(self.shape) == len(right), 'Incompatible stages'
        for src, dst in zip((o for n in self for o in n.apply), right):
            src.train(dst)
        return Tail(*right)

    def label(self, right: 'Head') -> 'Tail':
        """Connect apply output of this stage to label input of the right stage.

        Args:
            right: Other stage to which label input our apply output should be connected.

        Returns: right stage.
        """
        assert sum(self.shape) == len(right), 'Incompatible stages'
        for src, dst in zip((o for n in self for o in n.apply), right):
            src.label(dst)
        return Tail(*right)

    def state(self, right: 'Head') -> 'Tail':
        """Connect state output of this stage to state input of the right stage.

        Args:
            right: Other stage to which our state should be connected.

        Returns: right stage.
        """
        assert len(self) == len(right), 'Incompatible stages'
        for src, dst in zip(self, right):
            src.state.set(dst)
        return Tail(*right)


SENTINEL = Tail()


class Head(Abstract):
    """Group of nodes representing input stage that can become a subscriber to some other Tail stage(s).
    """
    class Set(set):
        """Set of heads.
        """
        def __init__(self, *heads: 'Head'):
            super().__init__(heads)

        def link(self, target: typing.Callable[['Head'], Tail]) -> None:
            for head in self:
                target(head)

    @property
    def shape(self) -> typing.Sequence[int]:
        """Shapes of input apply ports (numbers of individual apply inputs).

        Returns: Shapes of input apply ports.
        """
        return tuple(n.szin for n in self)


class Flow:
    """Stage flow representing passing of dataset through apply chain.
    """
    def __init__(self, heads: Head.Set, tail: typing.Optional[Tail] = None):
        self.head: Head.Set = heads
        self.tail: typing.Optional[Tail] = tail
