import typing

from forml import flow
from forml.flow import task
from forml.flow.graph import node


class Transparent(flow.Operator):

    def plan(self) -> flow.Plan:
        pass


class Transformer(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        self.instance = node.Factory(...)

        apply = node.Primitive(actor, szin=1, szout=1)
        train_train = node.Primitive(actor, szin=1, szout=1)
        train_apply = node.Primitive(actor, szin=1, szout=1)

    def plan(self) -> flow.Plan:
        instance = self.instance.new()
        apply: node.Primitive = instance.node()
        train_train: node.Primitive = instance.node()
        train_apply: node.Primitive = instance.node()

        left = self.left.plan()
        node.Condensed(train_apply, train_train)
        return flow.Plan(left.apply >> node.Condensed(apply),
                         node.Condensed(left.train.tail[0] >> train_apply[0],
                                        (left.train.tail[0], left.label.tail[0]) >> train_train),
                         left.label)


class Source(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Primitive(actor, szin=0, szout=1)
        # label extraction?
