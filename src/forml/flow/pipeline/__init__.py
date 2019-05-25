"""
ForML pipeline composition logic.
"""
import collections
import typing

from forml import flow
from forml.flow.graph import view, node, clean


class Error(flow.Error):
    """Custom pipeline error.
    """


class Segment(collections.namedtuple('Segment', 'apply, train, label')):
    """Structure for holding related flow parts of different modes.
    """
    def __new__(cls, apply: typing.Optional[view.Path] = None, train: typing.Optional[view.Path] = None,
                label: typing.Optional[view.Path] = None):
        return super().__new__(cls, apply or view.Path(node.Future()), train or view.Path(node.Future()),
                               label or view.Path(node.Future()))

    def extend(self, apply: typing.Optional[view.Path] = None,
               train: typing.Optional[view.Path] = None,
               label: typing.Optional[view.Path] = None) -> 'Segment':
        """Helper for creating new Track with specified paths extended by provided values.

        Args:
            apply: Optional path to be connected to apply segment.
            train: Optional path to be connected to train segment.
            label: Optional path to be connected to label segment.

        Returns: New Track instance.
        """
        return self.__class__(self.apply.extend(apply) if apply else self.apply,
                              self.train.extend(train) if train else self.train,
                              self.label.extend(label) if label else self.label)

    def use(self, apply: typing.Optional[view.Path] = None,
            train: typing.Optional[view.Path] = None,
            label: typing.Optional[view.Path] = None) -> 'Segment':
        """Helper for creating new Track with specified paths replaced by provided values.

        Args:
            apply: Optional path to be used as apply segment.
            train: Optional path to be used as train segment.
            label: Optional path to be used as label segment.

        Returns: New Track instance.
        """
        return self.__class__(apply or self.apply, train or self.train, label or self.label)


class Composition(collections.namedtuple('Composition', 'apply, train')):
    """Structure for holding related flow parts of different modes.
    """
    def __new__(cls, *segments: Segment):
        segments = iter(segments)
        composed = next(segments)
        for other in segments:
            composed = composed.extend(*other)

        apply = composed.apply.extend()
        # apply.accept(clean.Validator())
        train = composed.train.extend()
        train.accept(clean.Validator())
        # label = composed.label.extend()
        # label.accept(clean.Validator())
        return super().__new__(cls, apply, train)
