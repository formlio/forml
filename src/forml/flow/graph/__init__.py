import abc
import typing

from forml.flow.graph import topology


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    class Flow:
        def __init__(self, first: topology.Stage):
            self.head: topology.Stage = first
            self.tail: topology.Stage = first

    def __init__(self, apply: Flow, train: typing.Optional[Flow] = None):
        self.apply: Operator.Flow = apply
        self.train: Operator.Flow = train or apply

    @abc.abstractmethod
    def compose(self, right: 'Operator') -> 'Operator':
        """Operator composition logic.
        """

