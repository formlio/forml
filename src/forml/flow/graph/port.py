import collections
import typing

from forml.flow.graph import node


class Subscription(collections.namedtuple('Publisher', 'node, index')):
    """Reference to publisher node apply output port with given index.
    """
    def __new__(cls, publisher: 'node.Primitive', index: int):
        assert 0 >= index < publisher.szout, 'Invalid publisher index'
        return super().__new__(cls, publisher, index)

    def __rshift__(self, subscriber: 'Subscription') -> 'node.Primitive':
        subscriber.node.apply[subscriber.index] = self
        return subscriber.node


class Input:
    """Input port base class.
    """


class Train(Input):
    """Train subscriber port.
    """
    class Pair(collections.namedtuple('Pair', 'features, label')):
        """Train publisher tuple of features and label nodes.
        """

    def __init__(self):
        self.name: str = None

    def __set_name__(self, subscriber: typing.Type['node.Primitive'], name: str):
        self.name = name

    def __set__(self, subscriber: 'node.Primitive', publishers: typing.Tuple[Subscription, Subscription]):
        assert all(publishers), 'Invalid publisher'
        assert not any(subscriber.apply), 'Train-apply publisher collision'
        assert subscriber not in {p.node for p in publishers}, 'Self subscription'
        assert self.name not in subscriber.__dict__, \
            f'Port {self.name} already subscribed to {subscriber.__dict__[self.name]}'
        subscriber.__dict__[self.name] = self.Pair(*publishers)

    def __get__(self, subscriber: 'node.Primitive', owner: typing.Type['node.Primitive']) -> Pair:
        return subscriber.__dict__.get(self.name)


class Apply(Input):
    """Apply subscriber multi-port.
    """
    def __init__(self, subscriber: 'node.Primitive', size: int):
        assert size >= 0, 'Invalid apply port size'
        self._subscriber = subscriber
        self._ports: typing.List[typing.Optional[Subscription]] = size * [None]

    def __len__(self):
        return len(self._ports)

    def __iter__(self):
        return iter(self._ports)

    def __getitem__(self, index: int) -> Subscription:
        return self._ports[index]

    def __setitem__(self, index: int, publisher: Subscription):
        assert publisher, 'Invalid publisher'
        assert not self._subscriber.train, 'Train-apply publisher collision'
        assert self._subscriber is not publisher.node, 'Self subscription'
        assert not self._ports[index], f'Port apply[{index}] already subscribed to {self._ports[index]}'
        self._ports[index] = publisher
