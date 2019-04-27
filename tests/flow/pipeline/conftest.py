"""
Flow unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

import forml.flow.pipeline
from forml.flow.graph import node, view
from forml.flow.pipeline import topology
from forml.flow import task


@pytest.fixture(scope='function')
def operator(spec: task.Spec):
    """Operator fixture.
    """
    class Operator(topology.Operator):
        """Operator mock.
        """
        def compose(self, left: topology.Composable) -> forml.flow.pipeline.Segment:
            """Dummy composition.
            """
            track = left.expand()
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            extractor = node.Worker(spec, 1, 1)
            trainer.train(track.train.publisher, extractor[0])
            return track.use(label=track.train.extend(view.Path(extractor))).extend(view.Path(applier))

    return Operator()
