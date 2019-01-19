import typing

from forml import flow
from forml.flow import task, segment
from forml.flow.graph import node


class Transformer(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        super().__init__()
        self.actor = actor

    def compose(self, builder: segment.Builder) -> segment.Segment:
        apply: node.Worker = builder.node(self.actor, 1, 1)
        train_train: node.Worker = builder.node(self.actor, 1, 1)
        train_apply: node.Worker = builder.node(self.actor, 1, 1)

        left = builder.segment()
        train_train.train(left.train.publisher, left.label.publisher)
        left.apply.extend(node.Compound(apply))
        left.train.extend(node.Compound(train_apply))
        return left


class Source(flow.Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Worker(actor, szin=0, szout=1)
