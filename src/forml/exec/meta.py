import collections
import datetime
import typing

import forml
from forml.flow.graph import node

StateT = typing.TypeVar('StateT')
OrdinalT = typing.TypeVar('OrdinalT')


class Registry(typing.Generic[StateT]):
    def __init__(self, nodes: typing.Sequence[node.Worker], states: typing.Optional[typing.Sequence[StateT]] = None):
        assert not states or len(states) == len(nodes), 'Node-state cardinality mismatch'
        self._states: typing.Dict[node.Worker.GroupID, typing.Optional[StateT]] = collections.OrderedDict(
            (n.gid, s) for n, s in zip(nodes, states or len(nodes) * [None]))
        assert len(self._states) == len(nodes), 'Duplicate node GroupID'

    def get(self, gid: node.Worker.GroupID) -> StateT:
        return self._states[gid]

    def set(self, gid: node.Worker.GroupID, state: StateT) -> None:
        assert gid in self._states, 'Unknown node'
        self._states[gid] = state

    def __iter__(self) -> typing.Iterator[StateT]:
        return iter(self._states.values())


class Training(typing.Generic[StateT, OrdinalT], collections.namedtuple('Training', 'timestamp, ordinal, states')):
    """Collection for grouping training attributes.
    """
    def __new__(cls, timestamp: datetime.datetime, ordinal: OrdinalT, states: Registry[StateT]):
        return super().__new__(cls, timestamp, ordinal, tuple(states))


class Tuning(collections.namedtuple('Tuning', 'timestamp, score')):
    """Collection for grouping tuning attributes.
    """


class Package(typing.Generic[StateT, OrdinalT], collections.namedtuple('Package', 'project, training, tuning')):
    """Wrapper for persisting runtime package representation.
    """
    def __new__(cls, project: forml.Project, training: Training[StateT, OrdinalT],
                tuning: typing.Optional[Tuning] = None):
        return super().__new__(cls, project, training, tuning)
