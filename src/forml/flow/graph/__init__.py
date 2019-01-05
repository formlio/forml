import abc
import typing

from forml.flow import task
from forml.flow.graph.layout import node, stage


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    def __init__(self, apply: stage.Flow, train: stage.Flow, label: stage.Flow):
        self.apply: stage.Flow = apply
        self.train: stage.Flow = train
        self.label: stage.Flow = label

    @abc.abstractmethod
    def compose(self, right: 'Operator') -> 'Operator':
        """Operator composition logic.
        """


class Transformer(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        apply = node.Plain(actor, szin=1, szout=1)
        train_train = node.Plain(actor, szin=1, szout=1)
        train_apply = node.Plain(actor, szin=1, szout=1)

        train_train_tail = stage.Tail(train_train)
        train_train_head = stage.Head(train_train)
        train_apply_tail = stage.Tail(train_apply)
        train_apply_head = stage.Head(train_apply)
        train_train_tail.state(train_apply_head)

        super().__init__(stage.Flow(stage.Head.Set(stage.Head(apply)), stage.Tail(apply)),
                         stage.Flow(stage.Head.Set(train_train_head, train_apply_head), train_apply_tail),
                         stage.Flow(stage.Head.Set(train_train_head)))

    def compose(self, left: 'Operator') -> 'Operator':
        self.apply.head.link(left.apply.tail.apply)
        self.train.head.link(left.train.tail.train)
        self.label.head.link(left.label.tail.label)
        return Operator(stage.Flow(left.apply.head, self.apply.tail),
                        stage.Flow(left.train.head, self.train.tail),
                        left.label)


class Source(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Plain(actor, szin=0, szout=1)
        # label extraction?
