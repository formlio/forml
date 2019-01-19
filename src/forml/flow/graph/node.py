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

from forml.flow.graph import port


class Info(collections.namedtuple('Info', 'spec, instance')):
    """Worker node info type used for external reference.
    """
    def __str__(self):
        return f'{self.spec}#{self.instance}'


class Atomic(metaclass=abc.ABCMeta):
    """Abstract primitive task graph node.
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
        assert 0 <= index < self.szout, 'Invalid output index'
        assert self is not subscription.node, 'Self subscription'
        self._output[index].add(subscription)

    @abc.abstractmethod
    def copy(self) -> 'Atomic':
        """Create new node with same shape and actor as self but without any subscriptions.

        Returns: Copied node.
        """


class Worker(Atomic):
    """Main primitive node type.
    """
    def __init__(self, info: Info, szin: int, szout: int):
        super().__init__(szin, szout)
        self.info: Info = info

    def __str__(self):
        return str(self.info)

    def publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.

        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.

        Trained node must not be publishing.
        """
        assert not self.trained, 'Trained node publishing'
        super().publish(index, subscription)

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
        return Worker(self.info, self.szin, self.szout)


class Future(Atomic):
    """Fake transparent apply port node that can be used as a lazy publisher/subscriber that disappears
    from the chain once it gets connected to another apply node(s).
    """
    class PubSub(port.PubSub):
        """Overridden implementation that does the proxied publishing/subscription.
        """
        def __init__(self, node: 'Future', index: int,
                     register: typing.Callable[[port.Publishable], None],
                     sync: typing.Callable[[], None]):
            super().__init__(node, index)
            self._register: typing.Callable[[port.Publishable], None] = register
            self._sync: typing.Callable[[], None] = sync

        def subscribe(self, publisher: port.Publishable) -> None:
            """Register publisher for future subscriptions.

            Args:
                publisher: Actual left side publisher to be used for all the interim subscriptions.
            """
            self._register(publisher)
            self._sync()

    def __init__(self, szin: int = 1, szout: int = 1):
        super().__init__(szin, szout)
        self._proxy: typing.Dict[port.Publishable, int] = dict()

    def __getitem__(self, index) -> port.PubSub:
        def register(publisher: port.Publishable) -> None:
            """Callback for publisher proxy registration.

            Args:
                publisher: Left side publisher
            """
            assert publisher not in self._proxy, 'Publisher collision'
            self._proxy[publisher] = index

        return self.PubSub(self, index, register, self._sync)

    def _sync(self) -> None:
        """Callback for interconnecting proxied registrations.
        """
        for publisher, subscription in ((p, s) for p, i in self._proxy.items() for s in self._output[i]):
            publisher.republish(subscription)

    def publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.

        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.

        Upstream publish followed by proxy synchronization.
        """
        super().publish(index, subscription)
        self._sync()

    def copy(self) -> 'Future':
        """There is nothing to copy on a Future node so just create a new one.

        Returns: new Future node.
        """
        return Future(self.szin, self.szout)


class Factory(collections.namedtuple('Factory', 'info, szin, szout')):
    """Worker node factory for creating nodes representing same instance.
    """
    _INSTANCES: typing.Dict[typing.Hashable, int] = collections.defaultdict(int)

    def __new__(cls, spec: typing.Hashable, szin: int, szout: int):
        cls._INSTANCES[spec] += 1
        return super().__new__(cls, Info(spec, cls._INSTANCES[spec]), szin, szout)

    def node(self) -> Worker:
        """Create new node instance.

        Returns: Node instance.
        """
        return Worker(self.info, self.szin, self.szout)
