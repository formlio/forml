"""
Flow segment unit tests.
"""
# pylint: disable=no-self-use

from forml.flow import pipeline
from forml.flow.pipeline import topology


class TestComposition:
    """Composition unit tests.
    """

    def test_compose(self, operator: topology.Operator):
        """Test the pipeline.
        """
        pipeline.Composition(operator.expand())
