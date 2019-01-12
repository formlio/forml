import typing

from forml import flow
from forml.flow import task, Plan
from forml.flow.graph import node


class Transformer(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        self.instance = node.Factory(actor, ...)

    def plan(self) -> Plan:
        instance = self.instance.new()
        apply: node.Worker = instance.node()
        train_train: node.Worker = instance.node()
        train_apply: node.Worker = instance.node()

        left = self.left.plan()
        train_train.train(left.train.publisher, left.label.publisher)
        left.apply.expand(node.Condensed(apply))
        left.train.expand(node.Condensed(train_apply))
        return left


class Source(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Atomic(actor, szin=0, szout=1)
        # label extraction?
