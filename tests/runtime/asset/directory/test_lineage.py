"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

import forml
from forml.project import distribution
from forml.runtime.asset import directory as dirmod
from forml.runtime.asset.directory import root as rootmod, project as prjmod, lineage as lngmod
from . import Level


class TestVersion:
    """Lineage version unit tests.
    """
    def test_parse(self):
        """Parsing test.
        """
        ver = lngmod.Level.Key('0.1.dev2')
        lngmod.Level.Key(ver)
        lngmod.Level.Key(forml.__version__)
        with pytest.raises(lngmod.Level.Key.Invalid):
            lngmod.Level.Key('foobar')


class TestLevel(Level):
    """Lineage unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(directory: rootmod.Level,
               project_name: prjmod.Level.Key) -> typing.Callable[[typing.Optional[lngmod.Level.Key]], lngmod.Level]:
        """Parent fixture.
        """
        return lambda lineage: directory.get(project_name).get(lineage)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(populated_lineage: lngmod.Level.Key) -> lngmod.Level.Key:
        """Level fixture.
        """
        return populated_lineage

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_lineage: lngmod.Level.Key) -> lngmod.Level.Key:
        """Level fixture.
        """
        return last_lineage

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_lineage: lngmod.Level.Key) -> lngmod.Level.Key:
        """Level fixture.
        """
        return lngmod.Level.Key(f'{last_lineage.release[0] + 1}')

    def test_empty(self, parent: typing.Callable[[typing.Optional[lngmod.Level.Key]], lngmod.Level],
                   empty_lineage: lngmod.Level.Key):
        """Test default empty lineage generation retrieval.
        """
        generation = parent(empty_lineage).get()
        with pytest.raises(dirmod.Level.Listing.Empty):
            _ = generation.key
        assert not generation.tag.states

    def test_artifact(self, directory: rootmod.Level, project_name: prjmod.Level.Key, invalid_level: lngmod.Level.Key):
        """Registry take unit test.
        """
        with pytest.raises(dirmod.Level.Invalid):
            _ = directory.get(project_name).get(invalid_level).artifact

    def test_put(self, directory: rootmod.Level, project_name: prjmod.Level.Key, project_package: distribution.Package):
        """Registry put unit test.
        """
        with pytest.raises(dirmod.Level.Invalid):  # lineage already exists
            directory.get(project_name).put(project_package)
