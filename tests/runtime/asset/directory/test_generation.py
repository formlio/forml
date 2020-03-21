"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing
import uuid

import pytest

from forml.runtime.asset import directory as dirmod
from forml.runtime.asset.directory import root as rootmod, lineage as lngmod, generation as genmod
from . import Level


class TestLevel(Level):
    """Generation unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(directory: rootmod.Level, project_name: str, populated_lineage: lngmod.Version) -> typing.Callable[
            [typing.Optional[int]], genmod.Level]:
        """Parent fixture.
        """
        return lambda generation: directory.get(project_name).get(populated_lineage).get(generation)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(valid_generation: int) -> int:
        """Level fixture.
        """
        return valid_generation

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_generation: int) -> int:
        """Level fixture.
        """
        return last_generation

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_generation: int) -> int:
        """Level fixture.
        """
        return last_generation + 1

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_lineage(last_lineage: lngmod.Version) -> lngmod.Version:
        """Level fixture.
        """
        return lngmod.Version(f'{last_lineage.release[0] + 1}')

    def test_tag(self, directory: rootmod.Level, project_name: str,
                 project_lineage: lngmod.Version, empty_lineage: lngmod.Version,
                 valid_generation: int, tag: genmod.Tag):
        """Registry checkout unit test.
        """
        project = directory.get(project_name)
        with pytest.raises(dirmod.Level.Invalid):
            _ = project.get(empty_lineage).get(valid_generation).tag
        assert project.get(project_lineage).get(valid_generation).tag == tag
        assert project.get(empty_lineage).get(None).tag == genmod.Tag()

    def test_read(self, directory: rootmod.Level, project_name: str,
                  project_lineage: lngmod.Version, invalid_lineage: lngmod.Version,
                  valid_generation: int, states: typing.Mapping[uuid.UUID, bytes]):
        """Registry load unit test.
        """
        project = directory.get(project_name)
        with pytest.raises(dirmod.Level.Invalid):
            project.get(invalid_lineage).get(None).get(None)
        with pytest.raises(dirmod.Level.Invalid):
            project.get(project_lineage).get(valid_generation).get(None)
        for sid, value in states.items():
            assert project.get(project_lineage).get(valid_generation).get(sid) == value


class TestTag:
    """Generation tag unit tests.
    """
    def test_replace(self, tag: genmod.Tag):
        """Test replace strategies.
        """
        assert tag.replace(states=(1, 2, 3)).states == (1, 2, 3)
        with pytest.raises(ValueError):
            tag.replace(invalid=123)
        with pytest.raises(ValueError):
            tag.replace(training=123)
        with pytest.raises(ValueError):
            tag.replace(tuning=123)
        assert tag.training.replace(ordinal=123).training.ordinal == 123
        assert tag.tuning.replace(score=123).tuning.score == 123
        with pytest.raises(TypeError):
            tag.training.replace(invalid=123)

    def test_trigger(self, tag: genmod.Tag):
        """Test triggering.
        """
        trained = tag.training.trigger()
        assert trained.training.timestamp > tag.training.timestamp
        assert trained.tuning == tag.tuning
        tuned = tag.tuning.trigger()
        assert tuned.tuning.timestamp > tag.tuning.timestamp
        assert tuned.training == tag.training

    def test_bool(self):
        """Test the boolean mode values.
        """
        empty = genmod.Tag()
        assert not empty.training
        assert not empty.tuning
        assert empty.training.trigger().training
        assert empty.tuning.trigger().tuning

    def test_dumpload(self, tag: genmod.Tag):
        """Test tag serialization.
        """
        assert genmod.Tag.loads(tag.dumps()) == tag
