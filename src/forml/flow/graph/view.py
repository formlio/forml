"""
Graph view - useful lenses and manipulation of graph topology parts.
"""

import abc
import operator
import typing

from forml.flow.graph import node as grnode, port


class Visitor(metaclass=abc.ABCMeta):
    """View visitor interface.
    """
    @abc.abstractmethod
    def visit_path(self, head: grnode.Atomic, tail: grnode.Atomic) -> None:
        """Path visit.

        Args:
            head: Path head node.
            tail: Path tail node.
        """


class PreOrder(Visitor, metaclass=abc.ABCMeta):
    """Visitor iterating over all nodes between head and tail including all sink branches.
    """
    def visit_path(self, head: grnode.Atomic, tail: grnode.Atomic) -> None:
        """Path visit.

        Args:
            head: Path head node.
            tail: Path tail node.
        """
        def scan(publisher: grnode.Atomic, path: typing.FrozenSet[grnode.Atomic] = frozenset()) -> None:
            """Recursive path scan.

            Args:
                publisher: Node to be processed.
                path: Chain of nodes between current and head.
            """
            self.visit_node(publisher)
            seen.add(publisher)
            subscribers = {s.node for p in publisher.output for s in p if s.node not in seen and (
                publisher is not tail or not isinstance(s.port, port.Apply))}
            if not any(subscribers):
                return
            path = frozenset(path | {publisher})
            assert subscribers.isdisjoint(path), 'Cyclic flow'
            for node in subscribers:
                scan(node, path=path)

        seen = set()
        scan(head)

    @abc.abstractmethod
    def visit_node(self, node: grnode.Atomic) -> None:
        """Node processor.

        Args:
            node: Node being processed.
        """


class Path(tuple, metaclass=abc.ABCMeta):
    """Representing acyclic apply path(s) between two nodes - a sub-graph with single head and tail node each with
    at most one apply input/output port.

    This is a base and factory class for creating specific path instances.
    """

    _head: grnode.Atomic = property(operator.itemgetter(0))
    _tail: grnode.Atomic = property(operator.itemgetter(1))

    def __new__(cls, head: grnode.Atomic, tail: typing.Optional[grnode.Atomic] = None):
        def tailof(publisher: grnode.Atomic, path: typing.FrozenSet[grnode.Atomic] = frozenset()) -> grnode.Atomic:
            """Recursive traversing all apply subscription paths down to the tail checking there is just one.

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
            endings = {tailof(n, path=path) for n in subscribers}
            assert tail and tail in endings or not tail and len(endings) == 1, 'Unknown or invalid tail'
            return tail or endings.pop()

        assert head.szin in {0, 1}, 'Simple head required'
        tail = tailof(head)
        assert tail.szout in {0, 1}, 'Simple tail required'
        # pylint: disable=self-cls-assignment
        cls = Closure if any(isinstance(s.port, (port.Train, port.Label)) for p in tail.output for s in p) else Channel
        return super().__new__(cls, (head, tail))

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_path(self._head, self._tail)

    # @abc.abstractmethod
    def extend(self, right: typing.Optional['Path'] = None, tail: typing.Optional[grnode.Atomic] = None) -> 'Path':
        """Create new path by appending right head to our tail or traversing the graph to its actual tail.

        Args:
            right: Branch to extend with.
            tail: Optional tail as a path output vertex.

        Returns: New connected path.
        """
        raise NotImplementedError()

    def subscribe(self, publisher: port.Publishable) -> None:
        """Subscribe head node to given publisher.
        """
        self._head[0].subscribe(publisher)

    @property
    # @abc.abstractmethod
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """
        raise NotImplementedError()

    def copy(self) -> 'Path':
        """Make a copy of the apply path topology. Any nodes not on path are ignored.

        Returns: Copy of the apply path.
        """

        def mkcopy(publisher: grnode.Atomic, path: typing.FrozenSet[grnode.Atomic] = frozenset()) -> None:
            """Recursive path copy.

            Args:
                publisher: Node to be copied.
                path: Chain of nodes between current and head.

            Returns: Copy of the publisher node with all of it's subscriptions resolved.

            Only the main branch is copied ignoring all sink branches.
            """
            path = frozenset(path | {publisher})
            if publisher is self._tail:
                for orig in path:
                    pub = copies.get(orig) or copies.setdefault(orig, orig.copy())
                    for index, subscription in ((i, s) for i, p in enumerate(orig.output) for s in p if s.node in path):
                        sub = copies.get(subscription.node) or copies.setdefault(
                            subscription.node, subscription.node.copy())
                        pub[index].publish(sub, subscription.port)
            else:
                for subscriber in {s.node for p in publisher.output for s in p if isinstance(s.port, port.Apply)}:
                    assert subscriber not in path, 'Cyclic flow'
                    mkcopy(subscriber, path=path)

        copies = dict()
        mkcopy(self._head)
        return Path(copies.get(self._head), copies.get(self._tail))


class Channel(Path):
    """Path with regular output passing data through.
    """

    def extend(self, right: typing.Optional[Path] = None, tail: typing.Optional[grnode.Atomic] = None) -> Path:
        """Create new path by appending right head to our tail or retracing this path up to its physical or specified
        tail.

        Args:
            right: Optional path to extend with (retracing to physical or specified tail if not provided).
            tail: Optional tail as a path output vertex.

        Returns: New extended path.
        """
        # pylint: disable=protected-access
        if right:
            right._head[0].subscribe(self._tail[0])
            if not tail:
                tail = right._tail
        elif not tail:
            tail = Path(self._tail)._tail
        return Path(self._head, tail)

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """
        return self._tail[0].publisher


class Closure(Path):
    """Closure is a path with all of its output being published to train port(s) thus not passing anything through.
    Note based on the definition of tail node (having a apply output) this refers to the last apply node before final
    train port subscriber(s).
    """

    class Publishable(port.Publishable):
        """Customized Publishable verifying it's publishing only to Train ports.
        """
        def __init__(self, publisher: port.Publishable):
            super().__init__(None, None)
            self._publisher: port.Publishable = publisher

        def republish(self, subscription: port.Subscription) -> None:
            """Republish the subscription checking it's only for a train port.

            Args:
                subscription: Existing subscription descriptor.
            """
            assert isinstance(subscription.port, (port.Train, port.Label)), 'Closure path publishing'
            self._publisher.republish(subscription)

    def extend(self, right: typing.Optional[Path] = None, tail: typing.Optional[grnode.Atomic] = None) -> Path:
        """Closure path is not extendable.
        """
        if not right and not tail or tail is self._tail:
            return self
        raise AssertionError('Connecting closure path')

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation. Closure can only be published to Train ports.

        Returns: Publishable tail apply port reference.
        """
        return self.Publishable(self._tail[0].publisher)
