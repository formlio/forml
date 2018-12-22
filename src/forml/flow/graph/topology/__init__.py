"""
Graph topology entities.
"""
import collections
import typing

from forml.flow import task
from forml.flow.graph.topology import port


class Node:
    """Single task graph node.
    """
    def __init__(self, actor: typing.Type[task.Actor], szin: int, szout: int):
        self.uid: str = ...
        self.actor = actor
        self.szin: int = szin
        # output ports:
        self.apply: typing.Tuple[port.Data] = tuple(port.Data() for _ in range(szout))
        self.state: port.State = port.State()

    @property
    def szout(self) -> int:
        """Width of the output apply port.

        Returns: Output apply port width.
        """
        return len(self.apply)


class Stage:
    """Group of nodes representing same stage of the flow.
    """
    class Shape(collections.namedtuple('Shape', 'input, output')):
        """Shape tuple of input and output apply ports.
        """

    def __init__(self, nodes: typing.Iterable[Node]):
        self.nodes: typing.Sequence[Node] = tuple(nodes)

    @property
    def shape(self) -> Shape:
        """Shapes of input and output apply ports (numbers of individual apply inputs and outputs).

        Returns: Shapes of input and output apply ports.
        """
        return self.Shape(tuple(n.szin for n in self.nodes), tuple(n.szout for n in self.nodes))

    def apply(self, right: 'Stage') -> 'Stage':
        """Connect data (apply) output of this stage with data (apply) input of the right stage.

        Args:
            right: Other stage to which input our output should be connected.

        Returns: right stage
        """
        assert sum(self.shape.output) == sum(right.shape.input), 'Incompatible stages'
        for src, (dst, idx) in zip((o for n in self.nodes for o in n.apply),
                                   ((n, i) for n in right.nodes for i in range(n.szin))):
            src.apply(dst, idx)
        return right

    def train(self, right: 'Stage') -> 'Stage':
        """Connect apply output of this stage to train input of the right stage.

        Args:
            right: Other stage to which train input our apply output should be connected.

        Returns: right stage.
        """
        assert sum(self.shape.output) == len(right.nodes), 'Incompatible stages'
        for src, dst in zip((o for n in self.nodes for o in n.apply), right.nodes):
            src.train(dst)
        return right

    def label(self, right: 'Stage') -> 'Stage':
        """Connect apply output of this stage to label input of the right stage.

        Args:
            right: Other stage to which label input our apply output should be connected.

        Returns: right stage.
        """
        assert sum(self.shape.output) == len(right.nodes), 'Incompatible stages'
        for src, dst in zip((o for n in self.nodes for o in n.apply), right.nodes):
            src.label(dst)
        return right

    def state(self, right: 'Stage') -> 'Stage':
        """Connect state output of this stage to state input of the right stage.

        Args:
            right: Other stage to which our state should be connected.

        Returns: right stage.
        """
        assert len(self.nodes) == len(right.nodes), 'Incompatible stages'
        for src, dst in zip(self.nodes, right.nodes):
            src.state.set(dst)
        return right
