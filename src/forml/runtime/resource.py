"""
Specific metadata types used by the execution layer.
"""
import collections
import datetime
import typing
import uuid

import forml
from forml import etl
from forml.flow.graph import node as grnode, view



class Binding(collections.Iterable):
    """Mapping of nodes states.
    """
    class Nodes(view.PreOrder, collections.Sequence):
        """Path visitor for collecting all stateful nodes.
        """
        def __init__(self):
            self._nodes: typing.List[grnode.Worker] = list()

        def __len__(self):
            return len(self._nodes)

        def __getitem__(self, index: int) -> grnode.Worker:
            return self._nodes[index]

        def visit_node(self, node: grnode.Worker) -> None:
            """Process new node.

            Args:
                node: Node to be processed.
            """
            if node.trained:
                self._nodes.append(node)

    def __init__(self, nodes: typing.Sequence[grnode.Worker], states: typing.Optional[typing.Sequence[uuid.UUID]] = None):
        assert not states or len(states) == len(nodes), 'Node-state cardinality mismatch'
        self._states: typing.Dict[int, typing.Optional[uuid.UUID]] = collections.OrderedDict(
            (n.gid, s) for n, s in zip(nodes, states or len(nodes) * [None]))
        assert len(self._states) == len(nodes), 'Duplicate node'

    @classmethod
    def bind(cls, path: view.Path, states: typing.Optional[typing.Sequence[uuid.UUID]] = None) -> 'Binding':
        """Create the binding for all stateful nodes on given path and the optional set of their states.

        Args:
            path: Path containing stateful nodes.
            states: Optional states to be bound to nodes.

        Returns: Binding object.
        """
        nodes: Binding.Nodes = cls.Nodes()
        path.accept(nodes)
        return cls(nodes, states)

    def get(self, gid: int) -> uuid.UUID:
        """Get state for given Gid.

        Args:
            gid: Group ID to get the state for.

        Returns: State representation.
        """
        return self._states[gid]

    def set(self, gid: int, state: uuid.UUID) -> None:
        """Update the binding with new state for given Group ID.

        Args:
            gid: Group ID to be updated.
            state: New state to be used.
        """
        assert gid in self._states, 'Unknown node'
        self._states[gid] = state

    def __iter__(self) -> typing.Iterator[uuid.UUID]:
        return iter(self._states.values())


class Training(collections.namedtuple('Training', 'timestamp, ordinal')):
    """Collection for grouping training attributes.
    """
    def __new__(cls, timestamp: datetime.datetime, ordinal: float, states: Binding):
        return super().__new__(cls, timestamp, ordinal, tuple(states))


class Tuning(collections.namedtuple('Tuning', 'timestamp, score')):
    """Collection for grouping tuning attributes.
    """


class Record(collections.namedtuple('Record', 'training, tuning, states'))