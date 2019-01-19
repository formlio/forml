"""
ForML flow composition logic.
"""

import abc
import collections

from forml.flow import task, segment, graph
from forml.flow.graph import node


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
        """Expand the left segment.

        Returns: Operator composition plan.
        """

class Source:
    ...


class Composer:
    def __init__(self):
        self._id: str = ...
        self._flow: Pipeline = ...
        self._source: Source = ...
        # self._label: node.Worker = ... #(splitter 1:2) ???
        self._score = ...  # cv+metric -> single number
        self._report = ...  # arbitrary metrics -> kv list

    @property
    def train(self) -> graph.Path:
        """Training graph.

        Returns: Graph represented as compound node.
        """
        graph = self._source.copy()
        self._flow.train.subscriber.subscribe(graph)
        return graph

    @property
    def apply(self) -> graph.Path:
        """Apply graph.

        Returns: Graph represented as compound node.
        """
        graph = self._source.copy()
        self._flow.apply.subscriber.subscribe(graph)
        return graph

    @property
    def tune(self):
        """Tuning graph.

        Returns: Graph represented as compound node.
        """
        return None

    @property
    def score(self) -> graph.Path:
        """Scoring graph.

        Returns: Graph represented as compound node.
        """
        return None

    @property
    def report(self) -> graph.Path:
        """Reporting graph.

        Returns: Graph represented as compound node.
        """
        return None
