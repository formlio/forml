# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Graph node port functionality.
"""
import collections
import typing

from .. import _exception
from . import node as nodemod  # pylint: disable=unused-import


class Type(int):
    """Input port base class."""

    def __repr__(self):
        return f'{self.__class__.__name__}[{int(self)}]'

    def __hash__(self):
        return hash(self.__class__) ^ int.__hash__(self)

    def __eq__(self, other):
        return other.__class__ is self.__class__ and int.__eq__(self, other)


class Meta(type):
    """Metaclass for singleton types."""

    def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
        value = namespace.pop('VALUE')
        instance = None

        def new(cls):
            """Injected class new method ensuring singletons with static value are only created."""
            nonlocal instance
            if instance is None:
                instance = bases[0].__new__(cls, value)
            return instance

        namespace['__new__'] = new
        return super().__new__(mcs, name, bases, namespace)


class Train(Type, metaclass=Meta):
    """Train input port."""

    VALUE = 0


class Label(Type, metaclass=Meta):
    """Label input port."""

    VALUE = 1


class Apply(Type):
    """Apply input/output port at given index."""


class Subscription(collections.namedtuple('Subscription', 'node, port')):
    """Descriptor representing subscription node input port of given type."""

    # registry of ports subscribed on given node
    _PORTS: dict['nodemod.Atomic', set[Type]] = collections.defaultdict(set)  # TO-DO: switch to weakref

    def __new__(cls, subscriber: 'nodemod.Atomic', port: Type):
        if port in cls._PORTS[subscriber]:
            raise _exception.TopologyError('Double subscription')
        if cls._PORTS[subscriber] and (
            isinstance(port, Apply) ^ any(isinstance(s, Apply) for s in cls._PORTS[subscriber])
        ):
            raise _exception.TopologyError('Apply/Train collision')
        if isinstance(port, (Train, Label)) and any(subscriber.output):
            raise _exception.TopologyError('Publishing node trained')
        if isinstance(subscriber, nodemod.Future):
            raise _exception.TopologyError('Future node subscribing')
        cls._PORTS[subscriber].add(port)
        return super().__new__(cls, subscriber, port)

    def __repr__(self):
        return f'{self.node}@{self.port}'

    def __hash__(self):
        return hash(self.node) ^ hash(self.port)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self.node == other.node and self.port == other.port

    @classmethod
    def ports(cls, subscriber: 'nodemod.Atomic') -> typing.Iterable[Type]:
        """Get subscribed ports of given nodemod.

        Args:
            subscriber: Node whose subscribed ports should be retrieved.

        Returns:
            Subscribed ports of given nodemod.
        """
        return frozenset(cls._PORTS[subscriber])

    def __del__(self):
        self._PORTS.get(self.node, {}).discard(self.port)


class Applicable:
    """Base for publisher/subscriber proxies."""

    def __init__(self, node: 'nodemod.Atomic', index: int):
        self._node: 'nodemod.Atomic' = node
        self._index: int = index


class Publishable(Applicable):
    """Output apply port reference that can be used just for publishing."""

    @property
    def szout(self) -> int:
        """Size of publisher node output.

        Returns:
            Output size.
        """
        return self._node.szout

    def publish(self, subscriber: 'nodemod.Atomic', port: Type) -> None:
        """Publish new subscription.

        Args:
            subscriber: node to publish to
            port: port to publish to
        """
        if isinstance(subscriber, nodemod.Future) and subscriber is not self._node:
            subscriber[port].subscribe(self)
            return
        subscription = Subscription(subscriber, port)
        try:
            self.republish(subscription)
        except Exception as err:
            # TO-DO: use weakref
            Subscription._PORTS[subscriber].discard(port)  # pylint: disable=protected-access
            raise err

    def republish(self, subscription: Subscription) -> None:
        """Publish existing subscription.

        Args:
            subscription: Existing subscription descriptor.
        """
        self._node._publish(self._index, subscription)  # pylint: disable=protected-access


class Subscriptable(Applicable):
    """Input apply port reference that can be used just for subscribing."""

    @property
    def szin(self) -> int:
        """Size of publisher node input.

        Returns:
            Input size.
        """
        return self._node.szin

    def subscribe(self, publisher: Publishable) -> None:
        """Subscribe to give publisher.

        Args:
            publisher: Applicable to subscribe to.
        """
        publisher.publish(self._node, Apply(self._index))


class PubSub(Publishable, Subscriptable):
    """Input or output apply port reference that can be used for both subscribing and publishing."""

    @property
    def publisher(self) -> Publishable:
        """Return just a publishable representation.

        Returns:
            Publishable apply port reference.
        """
        return Publishable(self._node, self._index)

    @property
    def subscriber(self) -> Subscriptable:
        """Return just a subscriptable representation.

        Returns:
            Subscriptable apply port reference.
        """
        return Subscriptable(self._node, self._index)
