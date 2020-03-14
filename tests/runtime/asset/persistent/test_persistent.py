"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
from packaging import version

from forml.runtime.asset import persistent


class TestRegistry:
    """Registry unit tests.
    """

    def test_get(self, registry: persistent.Registry, project_name: str, populated_lineage: version.Version):
        """Test lineage get.
        """
        lineage = registry.get(project_name).get(populated_lineage)
        assert lineage.key == populated_lineage
