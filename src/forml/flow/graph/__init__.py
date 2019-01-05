import abc
import typing

from forml.flow import task
from forml.flow.graph.layout import node, stage


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    def __init__(self, apply: stage.Flow, train: typing.Optional[stage.Flow] = None):
        self.apply: stage.Flow = apply
        self.train: stage.Flow = train or apply

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

        super().__init__(stage.Flow(stage.Tail(apply), stage.Head(apply)),
                         stage.Flow(train_apply_tail, train_train_head, train_apply_head))



class Source(Operator):
    def __init__(self, actor: typing.Type[task.Actor]):
        node.Plain(actor, szin=0, szout=1)
        # label extraction?
