"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
from packaging import version

from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root as rootmod


class TestRegistry:
    """Registry unit tests.
    """

    def test_get(self, registry: persistent.Registry, project_name: str, populated_lineage: version.Version):
        """Test lineage get.
        """
        lineage = rootmod.Level(registry).get(project_name).get(populated_lineage)
        assert lineage.key == populated_lineage
