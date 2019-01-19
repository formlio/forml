"""
Flow graph entities.
"""

# TODO: use custom error type instead of assertions

import abc
import collections
import typing

from forml.flow.graph import node, port


class Path(collections.namedtuple('Path', 'head, tail'), metaclass=abc.ABCMeta):
    """Node representing compound acyclic flow - a sub-graph with single head and tail node each with at most one
    apply input/output port. This is a base and factory class for creating specific path instances.
    """

    def __new__(cls, head: node.Atomic, tail: typing.Optional[node.Atomic] = None):
        def tailof(publisher: node.Atomic, path: typing.FrozenSet[node.Atomic] = frozenset()) -> node.Atomic:
            """Recursive traversing all apply subscription paths up to the tail checking there is just one.

            Args:
                publisher: Start node for the traversal.
                path: Chain of nodes between current and head.

            Returns: Tail of the flow.
            """
            if tail and publisher is tail:
                return publisher
            subscribers = {s.node for p in publisher.output for s in p if isinstance(s.port, port.Apply)}
            if not any(subscribers):
                return publisher
            path = frozenset(path | {publisher})
            assert subscribers.isdisjoint(path), 'Cyclic flow'
            tails = {tailof(n, path=path) for n in subscribers}
            assert tail and tail in tails or not tail and len(tails) == 1
            return tail or tails.pop()

        assert head.szin in {0, 1}, 'Head node not condensable'
        tail = tailof(head)
        assert tail.szout in {0, 1}, 'Tail node not condensable'
        cls = Closure if any(isinstance(s.port, port.Train) for p in tail.output for s in p) else Channel
        return super().__new__(cls, head, tail)

    @abc.abstractmethod
    def extend(self, right: typing.Optional['Path'] = None) -> 'Path':
        """Create new path by appending right head to our tail.

        Args:
            right: Branch to extend with.

        Returns: New connected path.
        """

    @property
    def subscriber(self) -> port.Subscriptable:
        """Subscriptable head node representation.

        Returns: Subscriptable head apply port reference.
        """
        return self.head[0].subscriber

    @property
    @abc.abstractmethod
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """

    def copy(self) -> 'Path':
        """Make a copy of the apply path topology.

        Returns: Copy of the apply path.
        """

        def pathof(publisher: node.Atomic,
                   path: typing.FrozenSet[node.Atomic] = frozenset()) -> typing.FrozenSet[node.Atomic]:
            path = frozenset(path | {publisher})
            if publisher is self.tail:
                return path
            subscribers = {s.node for p in publisher.output for s in p if isinstance(s.port, port.Apply)}
            if not any(subscribers):
                return frozenset()
            assert subscribers.isdisjoint(path), 'Cyclic flow'
            return frozenset.union(*(pathof(n, path=path) for n in subscribers))

        def copyof(publisher: node.Atomic) -> node.Atomic:
            """Recursive copy resolver.

            Args:
                publisher: Node to be copied.

            Returns: Copy of the publisher node with all of it's subscriptions resolved.
            """
            if publisher not in mapping:
                copied = publisher.copy()
                if publisher is not self.tail:
                    for index, subscription in ((i, s) for i, p in enumerate(publisher.output) for s in p):
                        subscriber = mapping.get(subscription.node) or copyof(subscription.node)
                        copied[index].publish(subscriber, subscription.port)
                mapping[publisher] = copied
            return mapping[publisher]

        mapping = dict()
        return Path(copyof(self.head), copyof(self.tail))


class Channel(Path):
    """Path with regular output passing data through.
    """

    def extend(self, right: typing.Optional[Path] = None) -> Path:
        """Create new path by appending right head to our tail or retracing this path up to its physical tail.

        Args:
            right: Optional path to extend with (retracing to physical tail if not provided).

        Returns: New extended path.
        """
        tail = None
        if right:
            right.head[0].subscribe(self.tail[0])
            tail = right.tail
        return Path(self.head, tail)

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """
        return self.tail[0].publisher


class Closure(Path):
    """Closure is a path with all of its output being published to train port(s) thus not passing anything through.
    """

    class Publishable(port.Publishable):
        """Customized Publishable verifying it's publishing only to Train ports.
        """

        def republish(self, subscription: port.Subscription) -> None:
            """Republish the subscription checking it's only for a train port.


            Args:
                subscription: Existing subscription descriptor.
            """
            assert isinstance(subscription.port, port.Train), 'Closure path publishing'
            super().republish(subscription)

    def extend(self, right: typing.Optional[Path] = None) -> Path:
        """Closure path is not connectable.

        Args:
            right: Branch to extend with.
        """
        raise AssertionError('Connecting closure path')

    @property
    def publisher(self) -> Publishable:
        """Publishable tail node representation. Closure can only be published to Train ports.

        Returns: Publishable tail apply port reference.
        """
        return self.Publishable(self.tail, 0)
