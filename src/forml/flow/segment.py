import abc
import collections
import typing

from forml import flow
from forml.flow import task
from forml.flow.graph import node


class Segment(collections.namedtuple('Segment', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """


class Builder(metaclass=abc.ABCMeta):
    def __init__(self):
        self._group = 0

    @abc.abstractmethod
    def segment(self) -> Segment:
        pass

    def node(self, actor: typing.Type[task.Actor], szin: int, szout: int) -> node.Worker:
        # TODO: self._group
        return node.Worker(actor, szin, szout)


class Related:
    def __init__(self, operator: flow.Operator, builder: Builder):
        self._operator: flow.Operator = operator
        self._builder: Builder = builder


class Origin(Builder):
    def segment(self) -> Segment:
        return Segment(node.Compound(node.Future()), node.Compound(node.Future()), node.Compound(node.Future()))


class Recursive(Related, Builder):
    def __init__(self, operator: flow.Operator, builder: Builder):
        Builder.__init__(self)
        Related.__init__(self, operator, builder)

    def segment(self) -> Segment:
        self._builder._group += 1
        return self._operator.compose(self._builder)


class Link(Related):
    def __init__(self, operator, builder):
        super().__init__(operator, builder)

    def __rshift__(self, right: flow.Operator) -> 'Link':
        """Semantical composition construct.
        """
        return Link(right, Recursive(self._operator, self._builder))

    @property
    def flow(self) -> ...:
        segment = self._operator.compose(self._builder)
        assert not isinstance(segment.label, node.Future) or not any(
            segment.label.output), 'Label extraction missing'
        return XXX(segment.apply, segment.train)
