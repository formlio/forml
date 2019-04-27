"""
Flow unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml.flow import pipeline
from forml.flow import task
from forml.flow.graph import node, view
from forml.flow.pipeline import topology


@pytest.fixture(scope='function')
def operator(spec: task.Spec) -> topology.Operator:
    """Operator fixture.
    """
    class Operator(topology.Operator):
        """Operator mock.
        """
        def compose(self, left: topology.Composable) -> pipeline.Segment:
            """Dummy composition.
            """
            track = left.expand()
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            extractor = node.Worker(spec, 1, 1)
            trainer.train(track.train.publisher, extractor[0])
            return track.use(label=track.train.extend(view.Path(extractor))).extend(view.Path(applier))

    return Operator()


@pytest.fixture(scope='function')
def origin(spec: task.Spec) -> topology.Operator:
    """Origin operator fixture.
    """
    class Operator(topology.Operator):
        """Operator mock.
        """
        def compose(self, left: topology.Composable) -> pipeline.Segment:
            """Dummy composition.
            """
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            return pipeline.Segment(view.Path(applier), view.Path(trainer))

    return Operator()
