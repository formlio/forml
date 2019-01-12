import typing

from forml import flow
from forml.flow import task
from forml.flow.graph import node


class Stub(flow.Operator):
    def plan(self) -> flow.Plan:
        return flow.Plan(node.Compound(node.Future()),
                         node.Compound(node.Future()),
                         node.Compound(node.Future()))


class Transformer(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        super().__init__()
        self.instance = node.Factory(actor, ...)

    def plan(self) -> flow.Plan:
        instance = self.instance.new()
        apply: node.Worker = instance.node()
        train_train: node.Worker = instance.node()
        train_apply: node.Worker = instance.node()

        left = self.left.plan()
        train_train.train(left.train.publisher, left.label.publisher)
        left.apply.expand(node.Compound(apply))
        left.train.expand(node.Compound(train_apply))
        return left


class Source(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Atomic(actor, szin=0, szout=1)
        # label extraction?
