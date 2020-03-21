"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
import abc
import typing
import uuid

import pytest

from forml.project import distribution
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import lineage as lngmod, generation as genmod


class Registry(metaclass=abc.ABCMeta):
    """Base class for registry unit tests.
    """
    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='function')
    def constructor() -> typing.Callable[[], persistent.Registry]:
        """Registry fixture.
        """

    @staticmethod
    @pytest.fixture(scope='function')
    def empty(constructor: typing.Callable[[], persistent.Registry]) -> persistent.Registry:
        """Empty registry fixture.
        """
        return constructor()

    @staticmethod
    @pytest.fixture(scope='function')
    def populated(constructor: typing.Callable[[], persistent.Registry], project_package: distribution.Package,
                  project_name: str, project_lineage: lngmod.Version, valid_generation: int,
                  states: typing.Mapping[uuid.UUID, bytes], tag: genmod.Tag) -> persistent.Registry:
        """Populated registry fixture.
        """
        registry = constructor()
        registry.push(project_package)
        for sid, value in states.items():
            registry.write(project_name, project_lineage, sid, value)
        registry.close(project_name, project_lineage, valid_generation, tag)
        return registry

    def test_projects(self, empty: persistent.Registry, populated: persistent.Registry,
                      project_name: str):
        """Registry projects unit test.
        """
        assert not any(empty.projects())
        assert list(populated.projects()) == [project_name]

    def test_lineages(self, empty: persistent.Registry, populated: persistent.Registry,
                      project_name: str, project_lineage: lngmod.Version):
        """Registry lineages unit test.
        """
        assert not any(empty.lineages(project_name))
        assert list(populated.lineages(project_name)) == [project_lineage]

    def test_generations(self, empty: persistent.Registry, populated: persistent.Registry,
                         project_name: str, project_lineage: lngmod.Version, valid_generation: int):
        """Registry generations unit test.
        """
        assert not any(empty.lineages(project_name))
        assert list(populated.generations(project_name, project_lineage)) == [valid_generation]

    def test_push(self, empty: persistent.Registry, project_package: distribution.Package):
        """Registry put unit test.
        """
        empty.push(project_package)

    def test_mount(self, populated: persistent.Registry,
                   project_name: str, project_lineage: lngmod.Version, project_package: distribution.Package):
        """Registry take unit test.
        """
        assert populated.mount(project_name, project_lineage).package == project_package.manifest.package

    def test_read(self, populated: persistent.Registry, project_name: str,
                  project_lineage: lngmod.Version, valid_generation: int, states: typing.Mapping[uuid.UUID, bytes]):
        """Registry load unit test.
        """
        for sid, value in states.items():
            assert populated.read(project_name, project_lineage, valid_generation, sid) == value

    def test_open(self, populated: persistent.Registry, project_name: str,
                  project_lineage: lngmod.Version, valid_generation: int, tag: genmod.Tag):
        """Registry checkout unit test.
        """
        assert populated.open(project_name, project_lineage, valid_generation) == tag
