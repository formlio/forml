"""
Graph node entities.

Output ports:
* apply
* state

Input ports:
* hyper
* state
* apply (multi-port)
* train
* label

Each port can have at most one publisher.
Apply and train input port subscriptions are exclusive.
Trained node cannot be copied.
"""

import collections
import typing

from forml.flow import task


class Primitive:
    """Primitive task graph node.
    """
    class Port:
        def __init__(self):
            self.name: str = None

        def __set_name__(self, subscriber: typing.Type['Primitive'], name: str):
            self.name = name

        def __set__(self, subscriber: 'Primitive', publisher: 'Primitive'):
            assert not any(subscriber.apply) or self is not Primitive.train, 'Train-apply subscription collision'
            assert publisher is not subscriber, 'Self subscription'
            assert self.name not in subscriber.__dict__, \
                f'Port {self.name} already subscribed to {subscriber.__dict__[self.name]}'
            subscriber.__dict__[self.name] = publisher

        def __get__(self, subscriber: 'Primitive', owner: typing.Type['Primitive']) -> 'Primitive':
            return subscriber.__dict__.get(self.name)

    class ApplyPort:
        def __init__(self, subscriber: 'Primitive', size: int):
            assert size > 0, 'Invalid apply port size'
            self._subscriber = subscriber
            self._ports: typing.List[typing.Optional['Primitive']] = size * [None]

        def __len__(self):
            return len(self._ports)

        def __iter__(self):
            return iter(self._ports)

        def __getitem__(self, index: int) -> 'Primitive':
            return self._ports[index]

        def __setitem__(self, index: int, publisher: 'Primitive'):
            assert not self._subscriber.train, 'Train-apply subscription collision'
            assert publisher is not self._subscriber, 'Self subscription'
            assert not self._ports[index], f'Port apply[{index}] already subscribed to {self._ports[index]}'
            self._ports[index] = publisher

    hyper: Port = Port()
    state: Port = Port()
    train: Port = Port()
    label: Port = Port()

    def __init__(self, actor: typing.Type[task.Actor], szin: int, szout: int):
        assert szin > 0, 'Invalid node input size'
        assert szout > 0, 'Invalid node output size'
        self.uid: str = ...
        self.actor = actor
        self.szout: int = szout
        self._apply: Primitive.ApplyPort = self.ApplyPort(self, szin)

    @property
    def szin(self) -> int:
        """Width of the input apply port.

        Returns: Input apply port width.
        """
        return len(self._apply)

    @property
    def is_stateful(self) -> bool:
        """Node is stateful if it has a state subscription.

        Returns: True if stateful.
        """
        return bool(self.state)

    @property
    def apply(self) -> ApplyPort:
        """Getter for the apply multi-port subscriber.

        Returns: The apply multi-port.
        """
        return self._apply

    def copy(self) -> 'Primitive':
        """Create new node with same shape and actor as self and hyper-parameter and state subscriptions. Only apply
        nodes can be copied.

        Returns: Copied node.
        """
        assert not self.train, 'Copying trained node'
        copied = self.__class__(self.actor, self.szin, self.szout)
        copied.hyper = self.hyper
        copied.state = self.state
        return copied


class Condensed(collections.namedtuple('Condensed', 'head, tail')):
    """Node representing condensed acyclic flow - a sub-graph with single head and tail node each with single apply
    input/output port.
    """
    def __new__(cls, tail: Primitive):
        def headof(subscriber: Primitive, path: typing.FrozenSet[Primitive] = frozenset()) -> Primitive:
            """Recursive traversing all apply subscription paths up to the head checking there is just one.

            Args:
                subscriber: Start node for the traversal.
                path: Chain of nodes between current and tail.

            Returns: Head of the flow.
            """
            publishers = set(subscriber.apply)
            if not any(publishers):
                return subscriber
            assert all(publishers), 'Incomplete flow not condensable'
            path = frozenset(path | {subscriber})
            assert publishers.isdisjoint(path), 'Cyclic flow not condensable'
            heads = set(headof(p, path) for p in publishers)
            assert len(heads) == 1, 'Open flow not condensable'
            return heads.pop()

        assert tail.szout == 1, 'Tail node not condensable'
        head: Primitive = headof(tail)
        assert head.szin == 1, 'Head node not condensable'
        return super().__new__(cls, head, tail)

    def copy(self) -> 'Condensed':
        """Make a copy of the condensed topology.

        Returns: Copy of the condensed sub-graph.
        """
        def copyof(subscriber: Primitive) -> Primitive:
            """Recursive copy resolver.

            Args:
                subscriber: Node to be copied.

            Returns: Copy of the subscriber node with all of it's subscriptions resolved.
            """
            if subscriber not in mapping:
                copied = subscriber.copy()
                if subscriber is not self.head:
                    for index, publisher in enumerate(subscriber.apply):
                        copied.apply[index] = mapping.get(subscriber.apply[index]) or copyof(subscriber.apply[index])
                mapping[subscriber] = copied
            return mapping[subscriber]

        mapping = dict()
        return super().__new__(self.__class__, copyof(self.head), copyof(self.tail))


class Factory:
    pass