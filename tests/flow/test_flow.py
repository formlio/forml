"""
ForML flow unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow


class TestPipeline:
    """Pipeline unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def pipeline(operator: flow.Operator):
        """Pipeline fixture.
        """
        return flow.Pipeline(operator)

    def test_pipeline(self, pipeline):
        """Test the pipeline.
        """
        assert isinstance(pipeline, flow.Pipeline)
