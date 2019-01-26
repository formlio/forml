"""
Graph node port functionality.
"""
import collections
import typing

from forml.flow.graph import node as grnode  # pylint: disable=unused-import


class Type:
    """Input port base class.
    """


class SingletonMeta(type):
    """Metaclass for singleton types.
    """
    _INSTANCE = None

    def __call__(cls, *args, **kwargs):
        if not cls._INSTANCE:
            cls._INSTANCE = super().__call__(*args, **kwargs)
        return cls._INSTANCE


class Train(Type, metaclass=SingletonMeta):
    """Train input port.
    """


class Label(Type, metaclass=SingletonMeta):
    """Label input port.
    """


class Apply(Type, int):
    """Apply input/output port at given index.
    """


class Subscription(collections.namedtuple('Subscription', 'node, port')):
    """Descriptor representing subscription node input port of given type.
    """
    # registry of ports subscribed on given node
    _PORTS: typing.Dict['grnode.Atomic', typing.Set[Type]] = collections.defaultdict(set)

    def __new__(cls, subscriber: 'grnode.Atomic', port: Type):
        assert port not in cls._PORTS[subscriber], 'Already subscribed'
        assert not cls._PORTS[subscriber] or (isinstance(port, (Train, Label)) ^ any(
            isinstance(s, Apply) for s in cls._PORTS[subscriber])), 'Apply/Train collision'
        assert not isinstance(port, (Train, Label)) or not any(subscriber.output), 'Publishing node trained'
        cls._PORTS[subscriber].add(port)
        return super().__new__(cls, subscriber, port)

    def __hash__(self):
        return hash(self.node) ^ hash(self.port)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self.node is other.node and self.port is other.port

    @classmethod
    def ports(cls, subscriber: 'grnode.Atomic') -> typing.Iterable[Type]:
        """Get subscribed ports of given grnode.

        Args:
            subscriber: Node whose subscribed ports should be retrieved.

        Returns: Subscribed ports of given grnode.
        """
        return frozenset(cls._PORTS[subscriber])

    def __del__(self):
        self._PORTS.get(self.node, {}).discard(self.port)


class Applicable:
    """Base for publisher/subscriber proxies.
    """
    def __init__(self, node: 'grnode.Atomic', index: int):
        self._node: 'grnode.Atomic' = node
        self._index: int = index


class Publishable(Applicable):
    """Output apply port reference that can be used just for publishing.
    """
    @property
    def szout(self) -> int:
        """Size of publisher node output.

        Returns: Output size.
        """
        return self._node.szout

    def publish(self, subscriber: 'grnode.Atomic', port: Type) -> None:
        """Publish new subscription.

        Args:
            subscriber: node to publish to
            port: port to publish to
        """
        subscription = Subscription(subscriber, port)
        try:
            self.republish(subscription)
        except Exception as err:
            Subscription._PORTS[subscriber].discard(port)  # pylint: disable=protected-access
            raise err

    def republish(self, subscription: Subscription) -> None:
        """Publish existing subscription.

        Args:
            subscription: Existing subscription descriptor.
        """
        self._node.publish(self._index, subscription)


class Subscriptable(Applicable):
    """Input apply port reference that can be used just for subscribing.
    """
    @property
    def szin(self) -> int:
        """Size of publisher node input.

        Returns: Input size.
        """
        return self._node.szin

    def subscribe(self, publisher: Publishable) -> None:
        """Subscribe to give publisher.

        Args:
            publisher: Applicable to subscribe to.
        """
        publisher.publish(self._node, Apply(self._index))


class PubSub(Publishable, Subscriptable):
    """Input or output apply port reference that can be used for both subscribing and publishing.
    """
    @property
    def publisher(self) -> Publishable:
        """Return just a publishable representation.

        Returns: Publishable apply port reference.
        """
        return Publishable(self._node, self._index)

    @property
    def subscriber(self) -> Subscriptable:
        """Return just a subscriptable representation.

        Returns: Subscriptable apply port reference.
        """
        return Subscriptable(self._node, self._index)
