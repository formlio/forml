"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.runtime.asset import directory, persistent


class Level:
    """Common level functionality.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_level: int) -> int:
        """Level fixture.
        """
        return last_level + 1

    def test_default(self, parent: typing.Callable[[typing.Optional[int]], directory.Level], last_level: int):
        """Test default level retrieval.
        """
        assert parent(None).key == last_level

    def test_explicit(self, parent: typing.Callable[[typing.Optional[int]], directory.Level],
                      valid_level: int, invalid_level: int):
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
               project: str) -> typing.Callable[[typing.Optional[int]], directory.Lineage]:
        """Parent fixture.
        """
        return lambda lineage: registry.get(project, lineage)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(populated_lineage: int) -> int:
        """Level fixture.
        """
        return populated_lineage

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_lineage: int) -> int:
        """Level fixture.
        """
        return last_lineage

    def test_empty(self, parent: typing.Callable[[typing.Optional[int]], directory.Lineage], empty_lineage: int):
        """Test default empty lineage generation retrieval.
        """
        generation = parent(empty_lineage).get()
        with pytest.raises(directory.Level.Listing.Empty):
            _ = generation.key
        assert not generation.tag.states


class TestGeneration(Level):
    """Generation unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parent(registry: persistent.Registry, project: str,
               populated_lineage: int) -> typing.Callable[[typing.Optional[int]], directory.Generation]:
        """Parent fixture.
        """
        return lambda generation: registry.get(project, populated_lineage).get(generation)

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

    def test_tag(self, parent: typing.Callable[[typing.Optional[int]], directory.Generation], valid_generation: int,
                 tag: directory.Generation.Tag):
        """Test generation tag retrieval.
        """
        assert parent(valid_generation).tag == tag


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
