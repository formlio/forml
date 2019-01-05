import abc
import typing

from forml.flow import task
from forml.flow.graph.layout import node


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    class Flow:
        def __init__(self, head: node.Stage, tail: node.Stage):
            self.head: node.Stage = head
            self.tail: node.Stage = tail

    def __init__(self, apply: Flow, train: typing.Optional[Flow] = None):
        self.apply: Operator.Flow = apply
        self.train: Operator.Flow = train or apply

    @abc.abstractmethod
    def compose(self, right: 'Operator') -> 'Operator':
        """Operator composition logic.
        """


class Transformer(Operator):
    pass


class Source(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Plain(actor, szin=0, szout=1)
