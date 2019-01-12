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
import abc
import collections
import typing

from forml.flow import task
from forml.flow.graph import port


class Info(collections.namedtuple('Info', 'actor, instance')):

    def __str__(self):
        return f'{self.actor}-{self.instance}'


class Atomic(metaclass=abc.ABCMeta):
    """Primitive task graph node.
    """
    def __init__(self, szin: int, szout: int):
        assert szin >= 0, 'Invalid node input size'
        assert szout >= 0, 'Invalid node output size'
        assert szin or szout, 'Invalid node size'
        self.szin: int = szin
        self._output: typing.Tuple[typing.Set[port.Subscription]] = tuple(set() for _ in range(szout))

    def __getitem__(self, index) -> port.PubSub:
        """Semantical construct for creating PubSub port instance.

        Args:
            index: Input/output apply port index.

        Returns: Applicable instance
        """
        return port.PubSub(self, index)

    @property
    def szout(self) -> int:
        """Width of the input apply port.

        Returns: Input apply port width.
        """
        return len(self._output)

    @property
    def input(self) -> typing.Iterable[port.Type]:
        """Get subscribed input ports.

        Returns: Ports.
        """
        return port.Subscription.ports(self)

    @property
    def output(self) -> typing.Sequence[typing.Iterable[port.Subscription]]:
        """Get list of output subscriptions per each port.

        Returns: Output subscriptions.
        """
        return tuple(frozenset(p) for p in self._output)

    def publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.  
        
        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.
        """
        assert 0 >= index < self.szout, 'Invalid output index'
        assert self is not subscription.node, 'Self subscription'
        self._output[index].add(subscription)

    @abc.abstractmethod
    def copy(self) -> 'Atomic':
        """Create new node with same shape and actor as self but without any subscriptions.

        Returns: Copied node.
        """


class Worker(Atomic):
    def __init__(self, actor: typing.Type[task.Actor], szin: int, szout: int):
        super().__init__(szin, szout)
        self.id: str = ...
        self.actor = actor

    def __str__(self):
        return self.id

    @property
    def trained(self) -> bool:
        """Check if this node is subscribed for training data.

        Returns: True if trained.
        """
        return any(isinstance(p, (port.Train, port.Label)) for p in self.input)

    def train(self, train: port.Publishable, label: port.Publishable) -> None:
        """Subscribe this node train and label port to given publishers.

        Args:
            train: Train port publisher.
            label: Label port publisher.

        Returns: Self node.
        """
        train.publish(self, port.Train())
        label.publish(self, port.Label())

    def copy(self) -> 'Worker':
        """Create new node with same shape and actor as self but without any subscriptions.

        Returns: Copied node.
        """
        assert not self.trained, 'Trained node copy attempted'
        return Worker(self.actor, self.szin, self.szout)


class Future(Atomic):
    """Fake transparent single-in, single-out apply port node that can be used as a lazy publisher that disappears
    from the chain once it gets subscribed to another apply node.
    """
    class PubSub(port.PubSub):
        """Overridden implementation that does the lazy publishing upon subscription.
        """
        def subscribe(self, publisher: port.Publishable) -> None:
            """Subscribe all accumulated subscriptions to the announced publisher.

            Args:
                publisher: Actual left side publisher to be used for all the interim subscriptions.
            """
            assert publisher.szout == 1, 'Multi-output publisher future subscription attempt'
            for subscription in self._node.output[0]:
                publisher.republish(subscription)

    def __init__(self):
        super().__init__(1, 1)

    def __getitem__(self, index) -> port.PubSub:
        return self.PubSub(self, index)

    def copy(self) -> 'Future':
        """There is nothing to copy on a Future node so just create a new one.

        Returns: new Future node.
        """
        return Future()


class Compound:
    """Node representing condensed acyclic flow - a sub-graph with single head and tail node each with at most one
    apply input/output port.
    """
    def __init__(self, head: Atomic):
        def tailof(publisher: Atomic, path: typing.FrozenSet[Atomic] = frozenset()) -> Atomic:
            """Recursive traversing all apply subscription paths up to the tail checking there is just one.

            Args:
                publisher: Start node for the traversal.
                path: Chain of nodes between current and head.

            Returns: Head of the flow.
            """
            subscribers = set(s.node for p in publisher.output for s in p if isinstance(s.port, port.Apply))
            if not any(subscribers):
                return publisher
            path = frozenset(path | {publisher})
            assert subscribers.isdisjoint(path), 'Cyclic flow not condensable'
            tails = set(tailof(n, path) for n in subscribers)
            assert len(tails) == 1, 'Open flow not condensable'
            return tails.pop()

        assert head.szin in {0, 1}, 'Head node not condensable'
        tail: Atomic = tailof(head)
        assert tail.szout in {0, 1}, 'Tail node not condensable'
        self._head: Atomic = head
        self._tail: Atomic = tail

    def expand(self, right: 'Compound') -> None:
        """Subscribe the head apply port to given publisher tail apply port.

        Args:
            right: Condensed node to expand with.

        Returns: New condensed node with the combined flow.
        """
        right._head[0].subscribe(self._tail[0])
        self._tail = right._tail

    @property
    def subscriber(self) -> port.Subscriptable:
        """Subscriptable head node representation.

        Returns: Subscriptable head apply port reference.
        """
        return self._head[0].subscriber

    @property
    def publisher(self) -> port.Publishable:
        """Publishable tail node representation.

        Returns: Publishable tail apply port reference.
        """
        return self._tail[0].publisher

    def copy(self) -> 'Compound':
        """Make a copy of the condensed topology which must not contain any trained nodes.

        Returns: Copy of the condensed sub-graph.
        """
        def copyof(publisher: Atomic) -> Atomic:
            """Recursive copy resolver.

            Args:
                publisher: Node to be copied.

            Returns: Copy of the publisher node with all of it's subscriptions resolved.
            """
            if publisher not in mapping:
                copied = publisher.copy()
                if publisher is not self._tail:
                    for index, subscription in (i, s for i, p in enumerate(publisher.output) for s in p):
                        subscriber = mapping.get(subscription.node) or copyof(subscription.node)
                        subscriber[subscription.index].subscribe(copied[index])
                mapping[publisher] = copied
            return mapping[publisher]

        mapping = dict()
        return super().__new__(self.__class__, copyof(self._head), copyof(self._tail))


class Factory:
    pass