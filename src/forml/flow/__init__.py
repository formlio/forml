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
    def __new__(cls, composable: segment.Composable):
        track = composable.track()
        # pylint: disable=protected-access
        assert not isinstance(track.label._tail, node.Future) or not any(track.label._tail.output), 'Label not consumed'
        apply = track.apply.extend()
        train = track.train.extend()
        assert isinstance(apply, view.Channel), 'Apply path not a channel'
        assert isinstance(train._tail, node.Future) or isinstance(train, view.Closure), 'Train path not a closure'
        return super().__new__(cls, apply, train)


class Operator(segment.Composable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Task graph entity.
    """
    def track(self) -> segment.Track:
        """Create dummy composition of this operator on a future origin nodes.

        Returns: Segment track.
        """
        return self.compose(segment.Origin())


class Composer:
    """High-level flow composer.
    """
    def __init__(self):
        self._id: str = ...
        self._flow: Pipeline = ...
        self._source: 'Source' = ...
        # self._label: node.Worker = ... #(splitter 1:2) ???
        self._score = ...  # cv+metric -> single number
        self._report = ...  # arbitrary metrics -> kv list

    @property
    def train(self) -> view.Path:
        """Training lens.

        Returns: Graph represented as compound node.
        """
        graph = self._source.copy()
        self._flow.train.subscriber.subscribe(graph)
        return graph

    @property
    def apply(self) -> view.Path:
        """Apply lens.

        Returns: Graph represented as compound node.
        """
        graph = self._source.copy()
        self._flow.apply.subscriber.subscribe(graph)
        return graph

    @property
    def tune(self):
        """Tuning lens.

        Returns: Graph represented as compound node.
        """
        return None

    @property
    def score(self) -> view.Path:
        """Scoring lens.

        Returns: Graph represented as compound node.
        """
        return None

    @property
    def report(self) -> view.Path:
        """Reporting lens.

        Returns: Graph represented as compound node.
        """
        return None
