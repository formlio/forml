"""
Flow segment unit tests.
"""
# pylint: disable=no-self-use
import pytest

from forml.flow import pipeline, error
from forml.flow.pipeline import topology


class TestComposition:
    """Composition unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composition(origin: topology.Operator, operator: topology.Operator) -> pipeline.Composition:
        """Composition fixture.
        """
        return pipeline.Composition((origin >> operator).expand())

    def test_composition(self, origin: topology.Operator, operator: topology.Operator):
        """Test the pipeline.
        """
        with pytest.raises(error.Topology):  # contains Future node
            pipeline.Composition(operator.expand())

        pipeline.Composition((origin >> operator).expand())

    def test_shared(self, composition: pipeline.Composition):
        """Test the composition shared nodes.
        """
        assert any(composition.shared)
