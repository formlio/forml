from forml import flow
from forml.flow import task, segment
from forml.flow.graph import node


class Transformer(flow.Operator):
    def __init__(self, spec: task.Spec):
        super().__init__()
        self.spec = spec

    def compose(self, builder: segment.Builder) -> segment.Track:
        worker = node.Factory(self.spec, 1, 1)
        apply: node.Worker = worker.node()
        train_train: node.Worker = worker.node()
        train_apply: node.Worker = worker.node()

        left = builder.track()
        train_train.train(left.train.publisher, left.label.publisher)
        left.apply.extend(node.Compound(apply))
        left.train.extend(node.Compound(train_apply))
        return left


class Source(flow.Operator):
    def __init__(self, spec: task.Spec):
        ...
