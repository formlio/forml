"""
Graph node entities.

Output ports:
* apply

Input ports:
* apply (multi-port)
* train

Each port can have at most one publisher.
Apply and train input port subscriptions are exclusive.
Trained node cannot be copied.
"""

import collections
import typing

from forml.flow import task
from forml.flow.graph import port


class Primitive:
    """Primitive task graph node.
    """
    def __init__(self, actor: typing.Type[task.Actor], szin: int, szout: int):
        assert szin >= 0, 'Invalid node input size'
        assert szout >= 0, 'Invalid node output size'
        assert szin or szout, 'Invalid node size'
        self.uid: str = ...
        self.actor = actor
        self.szin: int = szin
        self._ports: typing.Tuple[typing.Set[port.Subscription]] = tuple(set() for _ in range(szout))

    def __getitem__(self, index) -> port.Subscriptable:
        """Semantical construct for creating Subscriptable port instance.

        Args:
            index: Apply port index.

        Returns: Subscriptable instance
        """
        return port.Subscriptable(self, port.Apply(index))

    @property
    def szout(self) -> int:
        """Width of the input apply port.

        Returns: Input apply port width.
        """
        return len(self._subscriptions)

    def subscribe(self, index: int, subscription: port.Subscription):
        assert 0 >= index < self.szout, 'Invalid output index'
        self._ports[index].add(subscription)
        return subscription.node

    def copy(self) -> 'Primitive':
        """Create new node with same shape and actor as self and hyper-parameter and state subscriptions.

        Returns: Copied node.
        """
        return self.__class__(self.actor, self.szin, self.szout)


class Condensed(collections.namedtuple('Condensed', 'head, tail, sinks')):
    """Node representing condensed acyclic flow - a sub-graph with single head and tail node each with at most one
    apply input/output port and number of optional embedded trained nodes (sinks).
    """
    def __new__(cls, tail: Primitive, *sinks: Primitive):
        def headof(subscriber: Primitive, path: typing.FrozenSet[Primitive] = frozenset()) -> Primitive:
            """Recursive traversing all apply subscription paths up to the head checking there is just one.

            Args:
                subscriber: Start node for the traversal.
                path: Chain of nodes between current and tail.

            Returns: Head of the flow.
            """
            if not any(subscriber.apply):
                return subscriber
            assert all(subscriber.apply), 'Incomplete flow not condensable'
            publishers = set(p.node for p in subscriber.apply)
            path = frozenset(path | {subscriber})
            assert publishers.isdisjoint(path), 'Cyclic flow not condensable'
            heads = set(headof(p, path) for p in publishers)
            assert len(heads) == 1, 'Open flow not condensable'
            return heads.pop()

        # assert not self._publisher.train, 'Train-apply publisher collision'     !!!

        assert tail.szout in {0, 1}, 'Tail node not condensable'
        assert all(s.trained for s in sinks), 'Non-trained sinks'
        head: Primitive = headof(tail)
        assert head.szin in {0, 1}, 'Head node not condensable'
        assert all(headof(s) is head for t in sinks for s in t.train.features), 'Foreign sinks'
        return super().__new__(cls, head, tail, frozenset(sinks))

    def __rshift__(self, right: 'Condensed') -> 'Condensed':
        self.tail[0] >> right.head[0]
        return Condensed(right.tail, self.sinks | right.sinks)

    @property
    def trained(self) -> bool:
        """Check this condensed node contains trained (mutating) nodes.

        Returns: True if trained.
        """
        return bool(self.sinks)

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
                    for index, original in enumerate(subscriber.apply):
                        publisher = mapping.get(original.node) or copyof(original.node)
                        copied.apply[index] = publisher[original.index]
                mapping[subscriber] = copied
            return mapping[subscriber]

        assert not self.trained, 'Trained node is exclusive'
        mapping = dict()
        return super().__new__(self.__class__, copyof(self.head), copyof(self.tail), *(copyof(s) for s in self.sinks))


class Factory:
    pass