"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest
from packaging import version

from forml.runtime.asset import directory, persistent
from forml.runtime.asset.directory import root as rootmod, generation as genmod


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
                   populated_lineage: version.Version, valid_generation: int, tag: genmod.Tag):
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


class Level:
    """Common level functionality.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def root(registry: persistent.Registry) -> rootmod.Level:
        """Directory root level fixture.
        """
        return rootmod.Level(registry)

    def test_default(self, parent: typing.Callable[[typing.Optional[directory.KeyT]], directory.Level],
                     last_level: directory.KeyT):
        """Test default level retrieval.
        """
        assert parent(None).key == last_level

    def test_explicit(self, parent: typing.Callable[[typing.Optional[directory.KeyT]], directory.Level],
                      valid_level: directory.KeyT, invalid_level: directory.KeyT):
        """Test explicit level retrieval.
        """
        assert parent(valid_level).key == valid_level
        with pytest.raises(directory.Level.Invalid):
            assert parent(invalid_level).key
