"""
Flow segment unit tests.
"""
# pylint: disable=no-self-use
import pytest

from forml.flow import pipeline, graph
from forml.flow.pipeline import topology


class TestComposition:
    """Composition unit tests.
    """

    def test_composition(self, origin: topology.Operator, operator: topology.Operator):
        """Test the pipeline.
        """
        with pytest.raises(graph.Error):  # contains Future node
            pipeline.Composition(operator.expand())

        pipeline.Composition((origin >> operator).expand())
