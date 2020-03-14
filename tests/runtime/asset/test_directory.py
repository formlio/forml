"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing
import uuid

from packaging import version
import pytest

from forml.project import distribution
from forml.runtime.asset import directory, persistent


class Level:
    """Common level functionality.
    """
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


class TestLineage(Level):
    """Lineage unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(registry: persistent.Registry,
               project_name: str) -> typing.Callable[[typing.Optional[version.Version]], directory.Lineage]:
        """Parent fixture.
        """
        return lambda lineage: registry.get(project_name).get(lineage)

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

    def test_empty(self, parent: typing.Callable[[typing.Optional[version.Version]], directory.Lineage],
                   empty_lineage: version.Version):
        """Test default empty lineage generation retrieval.
        """
        generation = parent(empty_lineage).get()
        with pytest.raises(directory.Level.Listing.Empty):
            _ = generation.key
        assert not generation.tag.states

    def test_artifact(self, registry: persistent.Registry, project_name: str, invalid_level: version.Version):
        """Registry take unit test.
        """
        with pytest.raises(directory.Level.Invalid):
            _ = registry.get(project_name).get(invalid_level).artifact

    def test_put(self, registry: persistent.Registry, project_name: str, project_package: distribution.Package):
        """Registry put unit test.
        """
        with pytest.raises(directory.Level.Invalid):  # lineage already exists
            registry.get(project_name).put(project_package)


class TestGeneration(Level):
    """Generation unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(registry: persistent.Registry, project_name: str, populated_lineage: version.Version) -> typing.Callable[
            [typing.Optional[int]], directory.Generation]:
        """Parent fixture.
        """
        return lambda generation: registry.get(project_name).get(populated_lineage).get(generation)

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
    def invalid_lineage(last_lineage: version.Version) -> version.Version:
        """Level fixture.
        """
        return version.Version(f'{last_lineage.release[0] + 1}')

    def test_tag(self, registry: persistent.Registry, project_name: str,
                 project_lineage: version.Version, empty_lineage: version.Version,
                 valid_generation: int, tag: directory.Generation.Tag):
        """Registry checkout unit test.
        """
        project = registry.get(project_name)
        with pytest.raises(directory.Level.Invalid):
            _ = project.get(empty_lineage).get(valid_generation).tag
        assert project.get(project_lineage).get(valid_generation).tag == tag
        assert project.get(empty_lineage).get(None).tag == directory.Generation.Tag()

    def test_read(self, registry: persistent.Registry, project_name: str,
                  project_lineage: version.Version, invalid_lineage: version.Version,
                  valid_generation: int, states: typing.Mapping[uuid.UUID, bytes]):
        """Registry load unit test.
        """
        project = registry.get(project_name)
        with pytest.raises(directory.Level.Invalid):
            project.get(invalid_lineage).get(None).get(None)
        with pytest.raises(directory.Level.Invalid):
            project.get(project_lineage).get(valid_generation).get(None)
        for sid, value in states.items():
            assert project.get(project_lineage).get(valid_generation).get(sid) == value


class TestTag:
    """Generation tag unit tests.
    """
    def test_replace(self, tag: directory.Generation.Tag):
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

    def test_trigger(self, tag: directory.Generation.Tag):
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
        empty = directory.Generation.Tag()
        assert not empty.training
        assert not empty.tuning
        assert empty.training.trigger().training
        assert empty.tuning.trigger().tuning

    def test_dumpload(self, tag: directory.Generation.Tag):
        """Test tag serialization.
        """
        assert directory.Generation.Tag.loads(tag.dumps()) == tag
