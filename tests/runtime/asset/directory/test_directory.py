"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import pytest

from forml.runtime.asset import directory, persistent
from forml.runtime.asset.directory import lineage as lngmod, generation as genmod


class TestCache:
    """Directory cache tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def cache() -> directory.Cache:
        """Cache fixture.
        """
        instance = directory.Cache(persistent.Registry.open)
        instance.clear()
        return instance

    def test_cycle(self, cache: directory.Cache, registry: persistent.Registry, project_name: str,
                   populated_lineage: lngmod.Version, valid_generation: int, tag: genmod.Tag):
        """Test the cache lifecycle.
        """
        assert cache.info.currsize == 0
        assert cache(registry, project_name, populated_lineage, valid_generation) == tag
        assert cache.info.misses == 1
        assert cache.info.currsize == 1
        assert cache(registry, project_name, populated_lineage, valid_generation) == tag
        assert cache.info.hits == 1
        cache.clear()
        assert cache.info.currsize == 0
