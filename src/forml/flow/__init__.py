"""
ForML flow composition logic.
"""

import abc
import collections
import typing

from forml.flow import task, segment
from forml.flow.graph import node, view


class Pipeline(collections.namedtuple('Pipeline', 'apply, train')):
    """Structure for holding related flow parts of different modes.
    """
    def __new__(cls, composable: segment.Composable):
        track = composable.track(node.Worker.Context())
        apply = track.apply.extend()
        assert isinstance(apply, view.Channel), 'Apply path not a channel'
        train = track.train.extend()
        assert isinstance(train, view.Closure), 'Train path not a closure'
        label = track.label.extend()
        assert isinstance(label, view.Closure), 'Label path not a closure'
        return super().__new__(cls, apply, train)


class Operator(segment.Composable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Task graph entity.
    """
    def track(self, context: node.Worker.Context) -> segment.Track:
        """Create dummy composition of this operator on a future origin nodes.

        Args:
            context: Worker context instance.

        Returns: Segment track.
        """
        return self.compose(context, segment.Origin())


class Composer:
    """High-level flow composer.
    """
    class Cached:
        """Path caching decorator.
        """
        def __init__(self, path: typing.Callable[['Composer'], view.Path]):
            self._path: typing.Callable[[Composer], view.Path] = path

        def __get__(self, composer: 'Composer', cls: typing.Type['Composer']):
            return composer.__dict__.setdefault(self._path.__name__, self._path(composer))

    def __init__(self, pipeline: segment.Composable):
        self._pipeline: Pipeline = Pipeline(pipeline)
        self._source: 'Source' = ...
        self._score = ...  # cv+metric -> single number
        self._report = ...  # arbitrary metrics -> kv list

    @Cached
    def train(self) -> view.Path:
        """Training lens.

        Returns: Graph represented as compound node.
        """
        source = self._source.copy()
        self._pipeline.train.subscribe(source)
        return source

    @Cached
    def apply(self) -> view.Path:
        """Apply lens.

        Returns: Graph represented as compound node.
        """
        source = self._source.copy()
        self._pipeline.apply.subscribe(source)
        return source

    @Cached
    def tune(self) -> view.Path:
        """Tuning lens.

        Returns: Graph represented as compound node.
        """
        ...

    @Cached
    def score(self) -> view.Path:
        """Scoring lens.

        Returns: Graph represented as compound node.
        """
        ...

    @Cached
    def report(self) -> view.Path:
        """Reporting lens.

        Returns: Graph represented as compound node.
        """
        ...
