"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root as rootmod, project as prjmod, lineage as lngmod


class TestRegistry:
    """Registry unit tests.
    """

    def test_get(self, registry: persistent.Registry, project_name: prjmod.Level.Key,
                 populated_lineage: lngmod.Level.Key):
        """Test lineage get.
        """
        lineage = rootmod.Level(registry).get(project_name).get(populated_lineage)
        assert lineage.key == populated_lineage
