"""
ForML flow composition logic.
"""

import abc
import collections

from forml.flow import task, segment
from forml.flow.graph import node, lens


class Pipeline(collections.namedtuple('Pipeline', 'apply, train')):
    """Structure for holding related flow parts of different modes.
    """


class Operator(metaclass=abc.ABCMeta):
    """Task graph entity.
    """
    def __rshift__(self, right: 'Operator') -> segment.Link:
        """Semantical composition construct.
        """
        return segment.Link(right, segment.Recursive(self, segment.Origin()))

    @abc.abstractmethod
    def compose(self, left: segment.Builder) -> segment.Track:
        """Expand the left segment producing new composed segment track.

        Returns: Composed segment track.
        """

class Composer:
    def __init__(self):
        self._id: str = ...
        self._flow: Pipeline = ...
        self._source: Source = ...
        # self._label: node.Worker = ... #(splitter 1:2) ???
        self._score = ...  # cv+metric -> single number
        self._report = ...  # arbitrary metrics -> kv list

    @property
    def train(self) -> lens.Path:
        """Training lens.

        Returns: Graph represented as compound node.
        """
        graph = self._source.copy()
        self._flow.train.subscriber.subscribe(graph)
        return graph

    @property
    def apply(self) -> lens.Path:
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
    def score(self) -> lens.Path:
        """Scoring lens.

        Returns: Graph represented as compound node.
        """
        return None

    @property
    def report(self) -> lens.Path:
        """Reporting lens.

        Returns: Graph represented as compound node.
        """
        return None
