"""
Runtime unit tests fixtures.
"""
# pylint: disable=no-self-use
import collections
import datetime
import typing
import uuid

import pytest

from forml import project as prjmod
from forml.runtime.asset import persistent, directory, access


@pytest.fixture(scope='function')
def states() -> typing.Mapping[uuid.UUID, bytes]:
    """States fixture.
    """
    return collections.OrderedDict(((uuid.UUID(bytes=b'\x00' * 16), b'\x00'),
                                    (uuid.UUID(bytes=b'\x01' * 16), b'\x01'),
                                    (uuid.UUID(bytes=b'\x02' * 16), b'\x02')))


@pytest.fixture(scope='function')
def tag(states: typing.Mapping[uuid.UUID, bytes]) -> directory.Generation.Tag:
    """Tag fixture.
    """
    return directory.Generation.Tag(training=directory.Generation.Tag.Training(datetime.datetime(2019, 4, 1), 123),
                                    tuning=directory.Generation.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
                                    states=states.keys())


@pytest.fixture(scope='session')
def project() -> str:
    """Project fixture.
    """
    return 'foo'


@pytest.fixture(scope='session')
def populated_lineage() -> int:
    """Lineage fixture.
    """
    return 1


@pytest.fixture(scope='session')
def empty_lineage() -> int:
    """Lineage fixture.
    """
    return 2


@pytest.fixture(scope='session')
def last_lineage(empty_lineage: int) -> int:
    """Lineage fixture.
    """
    return empty_lineage


@pytest.fixture(scope='session')
def valid_generation() -> int:
    """Generation fixture.
    """
    return 1


@pytest.fixture(scope='session')
def last_generation(valid_generation: int) -> int:
    """Generation fixture.
    """
    return valid_generation


@pytest.fixture(scope='function')
def content(populated_lineage: int, empty_lineage: int, valid_generation: int,
            tag: directory.Generation.Tag) -> typing.Mapping[int, typing.Mapping[int, directory.Generation.Tag]]:
    """Repo data content fixture.
    """
    return {populated_lineage: {valid_generation: tag}, empty_lineage: {}}


@pytest.fixture(scope='session')
def registry(content: typing.Mapping[int, typing.Mapping[int, directory.Generation.Tag]],
             states: typing.Mapping[uuid.UUID, bytes]) -> persistent.Registry:
    """Registry fixture.
    """
    class Registry(persistent.Registry):
        """Fixture registry implementation
        """
        def lineages(self, project: str) -> directory.Level.Listing:
            return directory.Level.Listing(content.keys())

        def generations(self, project: str, lineage: int) -> directory.Level.Listing:
            try:
                return directory.Level.Listing(content[lineage].keys())
            except KeyError:
                raise directory.Level.Invalid(f'Invalid lineage ({lineage})')

        def pull(self, project: str, lineage: int) -> prjmod.Artifact:
            raise NotImplementedError()

        def push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
            raise NotImplementedError()

        def read(self, project: str, lineage: int, generation: int, sid: uuid.UUID) -> bytes:
            if sid not in content[lineage][generation].states:
                raise directory.Level.Invalid(f'Invalid state id ({sid})')
            return states[sid]

        def write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
            raise NotImplementedError()

        def open(self, project: str, lineage: int, generation: int) -> directory.Generation.Tag:
            try:
                return content[lineage][generation]
            except KeyError:
                raise directory.Level.Invalid(f'Invalid generation ({lineage}.{generation})')

        def close(self, project: str, lineage: int, generation: int, tag: directory.Generation.Tag) -> None:
            raise NotImplementedError()

    return Registry()


@pytest.fixture(scope='function')
def valid_assets(registry: persistent.Registry, project: str, populated_lineage: int,
                 valid_generation: int) -> access.Assets:
    """Lineage fixture.
    """
    return access.Assets(registry, project, populated_lineage, valid_generation)
