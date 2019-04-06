"""
ForML flow unit tests.
"""
# pylint: disable=no-self-use

from forml import flow


class TestPipeline:
    """Pipeline unit tests.
    """

    def test_compose(self, operator: flow.Operator):
        """Test the pipeline.
        """
        flow.Pipeline.compose(operator.expand())
