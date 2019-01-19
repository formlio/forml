"""
Flow segments represent partial pipeline blocks during pipeline assembly.
"""

import abc
import collections
import typing

from forml import flow
from forml.flow import graph
from forml.flow.graph import node


class Track(collections.namedtuple('Track', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """
    def extend(self, apply: typing.Optional[graph.Path] = None,
               train: typing.Optional[graph.Path] = None,
               label: typing.Optional[graph.Path] = None) -> 'Track':
        """Helper for creating new extended Track.

        Args:
            apply: Optional branch to be connected to apply track.
            train: Optional branch to be connected to train track.
            label: Optional branch to be connected to label track.

        Returns: New Track instance.
        """
        return Track(self.apply.extend(apply) if apply else self.apply.extend(),
                     self.train.extend(train) if train else self.train.extend(),
                     self.label.extend(label) if label else self.label.extend())


class Builder(metaclass=abc.ABCMeta):
    """Builder of pipeline segment track.
    """
    @abc.abstractmethod
    def track(self) -> Track:
        """Build and return a sgment track.

        Returns: Segment track.
        """


class Related:
    """Generic base class for related types.
    """
    def __init__(self, operator: 'flow.Operator', builder: Builder):
        self._operator: 'flow.Operator' = operator
        self._builder: Builder = builder


class Origin(Builder):
    """Initial builder without a predecessor.
    """
    def track(self) -> Track:
        """Track of future nodes.

        Returns: Segment track.
        """
        return Track(graph.Path(node.Future()), graph.Path(node.Future()), graph.Path(node.Future()))


class Recursive(Related, Builder):
    def __init__(self, operator: 'flow.Operator', builder: Builder):
        Builder.__init__(self)
        Related.__init__(self, operator, builder)

    def track(self) -> Track:
        return self._operator.compose(self._builder)


class Link(Related):
    def __init__(self, operator, builder):
        super().__init__(operator, builder)

    def __rshift__(self, right: 'flow.Operator') -> 'Link':
        """Semantical composition construct.
        """
        return Link(right, Recursive(self._operator, self._builder))

    @property
    def pipeline(self) -> 'flow.Pipeline':
        track = self._operator.compose(self._builder)
        assert not isinstance(track.label, node.Future) or not any(track.label.output), 'Label not extracted'
        return flow.Pipeline(track.apply, track.train)
