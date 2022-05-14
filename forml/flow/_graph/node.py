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
Graph node entities.

Output ports:
* apply (multi-port)

Input ports:
* apply (multi-port)
* train
* label

Each port can have at most one publisher.
Apply and train input port subscriptions are exclusive.
Trained node cannot be copied.
"""
import abc
import collections
import typing
import uuid

from .. import _exception
from . import port

if typing.TYPE_CHECKING:
    from forml import flow


class Visitor:
    """View visitor interface."""

    def visit_node(self, node: 'Atomic') -> None:
        """Node visit.

        Args:
            node: Visited node.
        """


class Port(typing.Iterable[port.Subscription]):
    """Output port subscriptions as an ordered set.

    The ordering is a dependency for the equality comparison of two nodes.
    """

    def __init__(self):
        self._subscriptions: dict[port.Subscription, None] = collections.OrderedDict()

    def add(self, subscription: port.Subscription) -> None:
        """Add new subscription to this port.

        Args:
            subscription: Subscription to be registered.
        """
        self._subscriptions[subscription] = None

    def __iter__(self):
        return iter(self._subscriptions.keys())


class Atomic(metaclass=abc.ABCMeta):
    """Abstract primitive task graph node."""

    def __init__(self, szin: int, szout: int):
        if min(szin, szout) < 0 or szin == szout == 0:
            raise ValueError('Invalid node shape')
        self.szin: int = szin
        self.uid: uuid.UUID = uuid.uuid4()
        self._output: tuple[Port] = tuple(Port() for _ in range(szout))

    def __repr__(self):
        return f'{self.__class__.__name__}[uid={self.uid}]'

    def __getitem__(self, index) -> port.PubSub:
        """Semantical construct for creating PubSub port instance.

        Args:
            index: Input/output apply port index.

        Returns:
            Applicable instance
        """
        return port.PubSub(self, index)

    def __eq__(self, other: typing.Any) -> bool:
        """If each node is of different type the equality is based on the equality of their subscriptions. Otherwise the
        equality is based on object identity.

        Args:
            other: Object to compare with.

        Returns:
            True if equal.
        """
        if isinstance(other, Atomic) and other.__class__ is not self.__class__:
            return (
                self.szout == other.szout
                and any(self._output)
                and all(s == o for s, o in zip(self.output, other.output))
            )
        return id(self) == id(other)

    def __hash__(self) -> int:
        """We need a Future node to appear identical to a Worker node of same shape and subscriptions (so that the
        Future can represent a placeholder for that Worker). From that reason we need to hash both of these instances
        into same hashcode and the only attributes can distinguish them in that case is the shape.

        Returns:
            Node hashcode.
        """
        return hash(self.szin) ^ hash(self.szout)

    def accept(self, visitor: Visitor) -> None:
        """Visitor entrypoint.

        Args:
            visitor: Accepted visitor.
        """
        visitor.visit_node(self)

    @property
    def szout(self) -> int:
        """Width of the output apply port.

        Returns:
            Output apply port width.
        """
        return len(self._output)

    @property
    def output(self) -> typing.Sequence[typing.Iterable[port.Subscription]]:
        """Get list of output subscriptions per each port.

        Returns:
            Output subscriptions.
        """
        return tuple(tuple(s) for s in self._output)

    def _publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.

        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.
        """
        assert 0 <= index < self.szout, 'Invalid output index'
        if self is subscription.node:
            raise _exception.TopologyError('Self subscription')
        self._output[index].add(subscription)

    @abc.abstractmethod
    def subscribed(self, publisher: 'Atomic') -> bool:
        """Checking we are on given node's subscription list.

        Args:
            publisher: Node to check for being it's subscriber,

        Returns:
            True if we are given node's subscriber.
        """

    @abc.abstractmethod
    def fork(self) -> 'Atomic':
        """Create new node with same shape and actor as self but without any subscriptions.

        Returns:
            Forked node.
        """


class Worker(Atomic):
    """Main primitive node type."""

    class Group(set):
        """Container for holding all forked workers."""

        def __init__(self, spec: 'flow.Spec'):
            super().__init__()
            self.spec: 'flow.Spec' = spec
            self.uid: uuid.UUID = uuid.uuid4()

        def __repr__(self):
            return f'{self.spec}[uid={self.uid}]'

    @typing.overload
    def __init__(self, group_or_spec: 'flow.Spec', /, szin: int, szout: int):
        """Constructor for a new independent worker."""

    @typing.overload
    def __init__(self, group_or_spec: Group, /, szin: int, szout: int):
        """Constructor for a new worker belonging to the given group."""

    def __init__(self, group_or_spec, /, szin, szout):
        super().__init__(szin, szout)
        self._group: Worker.Group = (
            group_or_spec if isinstance(group_or_spec, Worker.Group) else self.Group(group_or_spec)
        )
        self._group.add(self)

    def __repr__(self):
        return repr(self._group)

    @property
    def spec(self) -> 'flow.Spec':
        """Task spec in this worker.

        Returns:
            Task spec.
        """
        return self._group.spec

    def _publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.

        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.

        Trained node must not be publishing.
        """
        if self.trained:
            raise _exception.TopologyError('Trained node publishing')
        super()._publish(index, subscription)

    @property
    def input(self) -> typing.Iterable[port.Type]:
        """Get subscribed input ports.

        Returns:
            Ports.
        """
        return port.Subscription.ports(self)

    @property
    def trained(self) -> bool:
        """Check if this node is subscribed for training data.

        Returns:
            True if trained.
        """
        return any(isinstance(p, (port.Train, port.Label)) for p in self.input)

    @property
    def stateful(self) -> bool:
        """Check this actor is stateful.

        Returns:
            True if stateful.
        """
        return self._group.spec.actor.is_stateful()

    @property
    def derived(self) -> bool:
        """Check this node is a state receiver in a trained group.

        Returns:
            True if persistent.
        """
        return self.stateful and any(n.trained for n in self.group if n is not self)

    @property
    def gid(self) -> uuid.UUID:
        """Return the group ID shared by all forks of this worker.

        Returns:
            Group ID.
        """
        return self._group.uid

    @property
    def group(self) -> typing.AbstractSet['Worker']:
        """Set of forked workers in the same fork group.

        Returns:
            Workers in same fork group.
        """
        return frozenset(self._group)

    def train(self, train: port.Publishable, label: port.Publishable) -> None:
        """Subscribe this node train and label port to given publishers.

        Args:
            train: Train port publisher.
            label: Label port publisher.

        Returns:
            Self node.
        """
        if not self.stateful:
            raise _exception.TopologyError('Stateless node training')
        if any(f.trained for f in self._group):
            raise _exception.TopologyError('Fork train collision')
        train.publish(self, port.Train())
        label.publish(self, port.Label())

    def subscribed(self, publisher: 'Atomic') -> bool:
        """Checking we are on given node's subscription list.

        Args:
            publisher: Node to check for being it's subscriber,

        Returns:
            True if we are given node's subscriber.
        """
        return any(s.node is self for p in publisher.output for s in p)

    def fork(self) -> 'Worker':
        """Create new node with same shape and actor as self but without any subscriptions.

        Returns:
            Forked node.
        """
        return Worker(self._group, self.szin, self.szout)

    @classmethod
    def fgen(cls, spec: 'flow.Spec', szin: int, szout: int) -> typing.Generator['Worker', None, None]:
        """Generator producing forks of the same node.

        Args:
            spec: Worker spec.
            szin: Worker input apply port size.
            szout: Worker output apply port size.

        Returns:
            Generator producing worker forks.
        """
        node = cls(spec, szin, szout)
        yield node
        while True:
            yield node.fork()


class Future(Atomic):
    """Fake transparent apply port node that can be used as a lazy publisher/subscriber that disappears
    from the chain once it gets connected to another apply node(s).
    """

    class PubSub(port.PubSub):
        """Overridden implementation that does the proxied publishing/subscription."""

        def __init__(
            self,
            node: 'Future',
            index: int,
            register: typing.Callable[[port.Publishable], None],
            sync: typing.Callable[[], None],
        ):
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
        self._proxy: dict[port.Publishable, int] = {}

    def __getitem__(self, index) -> port.PubSub:
        def register(publisher: port.Publishable) -> None:
            """Callback for publisher proxy registration.

            Args:
                publisher: Left side publisher
            """
            if publisher in self._proxy:
                raise _exception.TopologyError('Publisher collision')
            self._proxy[publisher] = index

        return self.PubSub(self, index, register, self._sync)

    def subscribed(self, publisher: 'Atomic') -> bool:
        """Overridden subscription checker. Future node checks the subscriptions in its proxy registrations.

        Args:
            publisher: Node to check for being it's subscriber,

        Returns:
            True if we are given node's subscriber.
        """
        # pylint: disable=protected-access
        return any(p._node is publisher or p._node.subscribed(publisher) for p in self._proxy)

    def _sync(self) -> None:
        """Callback for interconnecting proxied registrations."""
        for publisher, subscription in ((p, s) for p, i in self._proxy.items() for s in self._output[i]):
            publisher.republish(subscription)

    def _publish(self, index: int, subscription: port.Subscription) -> None:
        """Publish an output port based on the given subscription.

        Args:
            index: Output port index to publish from.
            subscription: Subscriber node and port to publish to.

        Upstream publish followed by proxy synchronization.
        """
        super()._publish(index, subscription)
        self._sync()

    def fork(self) -> 'Future':
        """There is nothing to copy on a Future node so just create a new one.

        Returns:
            new Future node.
        """
        return Future(self.szin, self.szout)
