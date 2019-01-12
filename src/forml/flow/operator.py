import abc
import typing

from forml import flow
from forml.flow import task
from forml.flow.graph import node


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    def __init__(self):
        self.left: Operator = Stub()

    def __rshift__(self, right: 'Operator') -> 'Operator':
        """Semantical construct for operator composition.
        """
        right.left = self
        return right

    @abc.abstractmethod
    def plan(self) -> flow.Plan:
        """Create and return new plan for this operator composition.

        Returns: Operator composition plan.
        """


class Stub(Operator):
    def plan(self) -> flow.Plan:
        return flow.Plan(node.Condensed(node.Future()),
                    node.Condensed(node.Future()),
                    node.Condensed(node.Future()))


class Transformer(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        self.instance = node.Factory(actor, ...)

    def plan(self) -> flow.Plan:
        instance = self.instance.new()
        apply: node.Worker = instance.node()
        train_train: node.Worker = instance.node()
        train_apply: node.Worker = instance.node()

        left = self.left.plan()
        train_train.train(left.train.publisher, left.label.publisher)
        left.apply.expand(node.Condensed(apply))
        left.train.expand(node.Condensed(train_apply))
        return left


class Source(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Atomic(actor, szin=0, szout=1)
        # label extraction?
