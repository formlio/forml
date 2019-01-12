import collections
import typing

from forml.flow.graph import node


class Input:
    """Input port base class.
    """

# SINGLETON!
class Train(Input):
    """Train input port.
    """


# SINGLETON!
class Label(Input):
    """Label input port.
    """


class Apply(Input, int):
    """Apply input/output port at given index.
    """


class Subscription(collections.namedtuple('Subscription', 'node, port')):
    """Reference to subscription node input port of given type.
    """
    def __hash__(self):
        return hash(self.node) ^ hash(self.port)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self.node is other.node and self.port is other.port


class Subscriptable(Subscription):
    """Reference to node apply input or output port.
    """
    def apply(self, publisher: 'Subscriptable') -> 'node.Primitive':
        publisher.node.publish(publisher.port, self)
        return self.node
