"""
ForML flow composition logic.
"""

import abc
import collections

from forml.flow import task, segment
from forml.flow.graph import node, view


class Pipeline(collections.namedtuple('Pipeline', 'apply, train')):
    """Structure for holding related flow parts of different modes.
    """
    def __new__(cls, track: segment.Track):
        apply = track.apply.extend()
        assert isinstance(apply, view.Channel), 'Apply path not a channel'
        train = track.train.extend()
        assert isinstance(train, view.Closure), 'Train path not a closure'
        label = track.label.extend()
        assert isinstance(label, view.Closure), 'Label path not a closure'
        return super().__new__(cls, apply, train)

    @classmethod
    def compose(cls, *tracks: segment.Track) -> 'Pipeline':
        """Compose the pipeline from the set of separate segments.

        Args:
            track: head segment of the pipeline.
            *downstream: All further segments.

        Returns: Pipeline instance.
        """
        if not tracks:
            raise ValueError('Missing tracks for composition')
        tracks = iter(tracks)
        composed = next(tracks)
        for other in tracks:
            composed = composed.extend(other.apply, other.train, other.label)
        return cls(composed)


class Operator(segment.Composable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Task graph entity.
    """
    def expand(self) -> segment.Track:
        """Create dummy composition of this operator on a future origin nodes.

        Args:
            context: Worker context instance.

        Returns: Segment track.
        """
        return self.compose(segment.Origin())
