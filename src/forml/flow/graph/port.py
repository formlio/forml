import collections
import typing
from typing import Any

from forml.flow.graph import node


class Type:
    """Input port base class.
    """


class SingletonMeta(type):
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
    _PORTS: typing.Dict['node.Primitive', typing.Set[Type]] = collections.defaultdict(set)

    def __new__(cls, subscriber: 'node.Primitive', port: Type):
        assert port not in cls._PORTS[subscriber], 'Already subscribed'
        assert isinstance(port, (Train, Label)) ^ any(
            isinstance(s, Apply) for s in cls._PORTS[subscriber]), 'Apply/Train collision'
        cls._PORTS[subscriber].add(port)
        return super().__new__(cls, subscriber, port)

    def __hash__(self):
        return hash(self.node) ^ hash(self.port)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self.node is other.node and self.port is other.port

    @classmethod
    def ports(cls, subscriber: 'node.Primitive') -> typing.Iterable[Type]:
        """Get subscribed ports of given node.

        Args:
            subscriber: Node whose subscribed ports should be retrieved.

        Returns: Subscribed ports of given node.
        """
        return frozenset(cls._PORTS[subscriber])


class Applicable:
    """Base for publisher/subscriber proxies.
    """
    def __init__(self, nod: node.Atomic, index: int):
        self._node: 'node.Primitive' = nod
        self._index: int = index


class Publishable(Applicable):
    """Output apply port reference that can be used just for publishing.
    """
    @property
    def szout(self) -> int:
        return self._node.szout

    def publish(self, subscriber: 'node.Primitive', port: Type) -> None:
        """Publish new subscription.

        Args:
            subscriber: node to publish to
            port: port to publish to
        """
        self.republish(Subscription(subscriber, port))

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
