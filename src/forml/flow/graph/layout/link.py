"""
Plain port types.
"""

import typing

from forml.flow.graph.layout import node


class Input:
    """Event subscriber defined as a node input port.
    """
    class Type:
        """Base input port type.
        """
        def __init__(self, name: str):
            self.name = name

    class Apply(Type):
        """Apply input is a vector so as a port type needs special class to support targeting particular index.
        """
        _NAME = 'apply'

        def __init__(self, index: int):
            super().__init__(self._NAME)
            self.index = index

    TRAIN = Type('train')  # train features input port
    LABEL = Type('label')  # train label input port
    STATE = Type('state')  # state input port
    HYPER = Type('hyper')  # params input port

    def __init__(self, node: 'node.Plain', port: Type):
        self.node = node
        self.port = port


class Output:
    """Event publisher representing a node output port (apply or state).
    """
    def __init__(self):
        self._links: typing.List[Input] = list()


class Data(Output):
    """Data output port represents the result of the apply mode. It can be linked to any input port type apart
    from state port.
    """
    def train(self, node: 'node.Plain') -> None:
        """Link this output port to given node's train input port.

        Args:
            node: target node to link to.
        """
        self._links.append(Input(node, Input.TRAIN))

    def label(self, node: 'node.Plain') -> None:
        """Link this output port to given node's label input port.

        Args:
            node: target node to link to.
        """
        self._links.append(Input(node, Input.LABEL))

    def apply(self, node: 'node.Plain', index: int) -> None:
        """Link this output port to given node's apply input port at given index.

        Args:
            node: target node to link to.
            index: target node apply port index to link to.
        """
        self._links.append(Input(node, Input.Apply(index)))

    def hyper(self, node: 'node.Plain') -> None:
        """Link this output port to given node's hyper params input port.

        Args:
            node: target node to link to.
        """
        self._links.append(Input(node, Input.HYPER))


class State(Output):
    """State output port can be linked only to state input ports.
    """

    def set(self, node: 'node.Plain') -> None:
        """Register new subscriber to this port.

        Args:
            node: Target node to link to.
        """
        self._links.append(Input(node, Input.STATE))
