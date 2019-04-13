"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use

from forml.runtime.asset import persistent


class TestRegistry:
    """Registry unit tests.
    """

    def test_get(self, registry: persistent.Registry, project:str, populated_lineage: int):
        """Test lineage get.
        """
        lineage = registry.get(project, populated_lineage)
        assert lineage.key == populated_lineage
