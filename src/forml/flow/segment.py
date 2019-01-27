"""
Flow segments represent partial pipeline blocks during pipeline assembly.
"""

import abc
import collections
import typing

from forml import flow
from forml.flow.graph import node, view


class Track(collections.namedtuple('Track', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """
    def __new__(cls, apply: typing.Optional[view.Path] = None,
                train: typing.Optional[view.Path] = None,
                label: typing.Optional[view.Path] = None):
        # pylint: disable=abstract-class-instantiated
        return super().__new__(cls, apply or view.Path(node.Future()),
                               train or view.Path(node.Future()),
                               label or view.Path(node.Future()))

    def extend(self, apply: typing.Optional[view.Path] = None,
               train: typing.Optional[view.Path] = None,
               label: typing.Optional[view.Path] = None) -> 'Track':
        """Helper for creating new Track with all paths extended either with provided or automatic values.

        Args:
            apply: Optional path to be connected to apply track.
            train: Optional path to be connected to train track.
            label: Optional path to be connected to label track.

        Returns: New Track instance.
        """
        return Track(self.apply.extend(apply), self.train.extend(train), self.label.extend(label))

    def use(self, apply: typing.Optional[view.Path] = None,
            train: typing.Optional[view.Path] = None,
            label: typing.Optional[view.Path] = None) -> 'Track':
        """Helper for creating new Track with paths replaced with provided values or left original.

        Args:
            apply: Optional path to be used as apply track.
            train: Optional path to be used as train track.
            label: Optional path to be used as label track.

        Returns: New Track instance.
        """
        return Track(apply or self.apply, train or self.train, label or self.label)


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
        return Track()


class Recursive(Related, Builder):
    """Builder further in the linked chain.
    """
    def __init__(self, operator: 'flow.Operator', builder: Builder):
        Builder.__init__(self)
        Related.__init__(self, operator, builder)

    def track(self) -> Track:
        """Track of future nodes.

        Returns: Segment track.
        """
        return self._operator.compose(self._builder)


class Link(Related):
    """Operator chaining descriptor.
    """
    def __rshift__(self, right: 'flow.Operator') -> 'Link':
        """Semantical composition construct.
        """
        return Link(right, Recursive(self._operator, self._builder))

    @property
    def pipeline(self) -> 'flow.Pipeline':
        """Compose the while chain returning a Pipeline instance.

        Returns: Pipeline instance.
        """
        track = self._operator.compose(self._builder)
        assert not isinstance(track.label, node.Future) or not any(track.label.output), 'Label not extracted'
        return flow.Pipeline(track.apply, track.train)
