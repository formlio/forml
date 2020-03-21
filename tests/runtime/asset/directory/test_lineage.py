"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest
from packaging import version

from forml.project import distribution
from forml.runtime.asset import directory
from forml.runtime.asset.directory import lineage as lngmod, root as rootmod
from . import Level


class TestLevel(Level):
    """Lineage unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(root: rootmod.Level,
               project_name: str) -> typing.Callable[[typing.Optional[version.Version]], lngmod.Level]:
        """Parent fixture.
        """
        return lambda lineage: root.get(project_name).get(lineage)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(populated_lineage: version.Version) -> version.Version:
        """Level fixture.
        """
        return populated_lineage

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_lineage: version.Version) -> version.Version:
        """Level fixture.
        """
        return last_lineage

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_lineage: version.Version) -> version.Version:
        """Level fixture.
        """
        return version.Version(f'{last_lineage.release[0] + 1}')

    def test_empty(self, parent: typing.Callable[[typing.Optional[version.Version]], lngmod.Level],
                   empty_lineage: version.Version):
        """Test default empty lineage generation retrieval.
        """
        generation = parent(empty_lineage).get()
        with pytest.raises(directory.Level.Listing.Empty):
            _ = generation.key
        assert not generation.tag.states

    def test_artifact(self, root: rootmod.Level, project_name: str, invalid_level: version.Version):
        """Registry take unit test.
        """
        with pytest.raises(directory.Level.Invalid):
            _ = root.get(project_name).get(invalid_level).artifact

    def test_put(self, root: rootmod.Level, project_name: str, project_package: distribution.Package):
        """Registry put unit test.
        """
        with pytest.raises(directory.Level.Invalid):  # lineage already exists
            root.get(project_name).put(project_package)
