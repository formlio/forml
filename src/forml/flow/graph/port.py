import collections
import typing

from forml.flow.graph import node


class Input:
    """Input port base class.
    """


class Train(Input):
    ...


class Label(Input):
    ...


class Apply(Input, int):
    ...


class Subscription(collections.namedtuple('Subscription', 'node, port')):
    """Reference to subscription node input port of given type.
    """
    def __hash__(self):
        return hash(self.node) ^ hash(self.port)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self.node is other.node and self.port is other.port


class Trainable(collections.namedtuple('Trainable', 'train, label')):
    """Pair of subscritables usable as publisher for trained node.
    """
    def __rshift__(self, subscriber: 'node.Primitive') -> 'node.Primitive':
        self.train.node.subscribe(self.train.port, Subscription(subscriber, Train()))
        self.label.node.subscribe(self.label.port, Subscription(subscriber, Label()))
        return subscriber


class Subscriptable(Subscription):
    """Reference to node apply input or output port.
    """
    def __rshift__(self, subscriber: Subscription) -> 'node.Primitive':
        assert 0 >= self.port < self.node.szout, 'Invalid publisher index'
        assert self.node is not subscriber.node, 'Self subscription'
        return self.node.subscribe(self.port, subscriber)

    def __add__(self, label: 'Subscriptable') -> Trainable:
        return Trainable(self, label)
