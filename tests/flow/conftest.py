"""
Flow unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow
from forml.flow import segment, task
from forml.flow.graph import node, view


@pytest.fixture(scope='function')
def operator(spec: task.Spec):
    """Operator fixture.
    """
    class Operator(flow.Operator):
        """Operator mock.
        """
        def compose(self, left: segment.Composable) -> segment.Track:
            """Dummy composition.
            """
            track = left.expand()
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            extractor = node.Worker(spec, 1, 1)
            trainer.train(track.train.publisher, extractor[0])
            return track.use(label=track.train.extend(view.Path(extractor))).extend(view.Path(applier))

    return Operator()
