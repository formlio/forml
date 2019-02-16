import collections
import datetime
import typing

import forml
from forml import etl
from forml.flow.graph import node as grnode, view

StateT = typing.TypeVar('StateT')


class Registry(typing.Generic[StateT]):
    class Stateful(view.PreOrder):
        """Path visitor for collecting all stateful nodes.
        """

        def __init__(self, *args, **kwargs):
            self._nodes: typing.List[grnode.Worker] = list()

        def visit_node(self, node: grnode.Worker) -> None:
            """Process new node.

            Args:
                node: Node to be processed.
            """
            if node.trained:
                self._nodes.append(node)



    def __init__(self, nodes: typing.Sequence[grnode.Worker], states: typing.Optional[typing.Sequence[StateT]] = None):
        assert not states or len(states) == len(nodes), 'Node-state cardinality mismatch'
        self._states: typing.Dict[grnode.Worker.Group.ID, typing.Optional[StateT]] = collections.OrderedDict(
            (n.gid, s) for n, s in zip(nodes, states or len(nodes) * [None]))
        assert len(self._states) == len(nodes), 'Duplicate node Group.IdT'

    def get(self, gid: grnode.Worker.Group.ID) -> StateT:
        return self._states[gid]

    def set(self, gid: grnode.Worker.Group.ID, state: StateT) -> None:
        assert gid in self._states, 'Unknown node'
        self._states[gid] = state

    def __iter__(self) -> typing.Iterator[StateT]:
        return iter(self._states.values())


class Training(typing.Generic[StateT, etl.OrdinalT], collections.namedtuple('Training', 'timestamp, ordinal, states')):
    """Collection for grouping training attributes.
    """
    def __new__(cls, timestamp: datetime.datetime, ordinal: etl.OrdinalT, states: Registry[StateT]):
        return super().__new__(cls, timestamp, ordinal, tuple(states))


class Tuning(collections.namedtuple('Tuning', 'timestamp, score')):
    """Collection for grouping tuning attributes.
    """


class Package(typing.Generic[StateT, etl.OrdinalT], collections.namedtuple('Package', 'project, training, tuning')):
    """Wrapper for persisting runtime package representation.
    """
    def __new__(cls, project: forml.Project, training: Training[StateT, etl.OrdinalT],
                tuning: typing.Optional[Tuning] = None):
        return super().__new__(cls, project, training, tuning)
