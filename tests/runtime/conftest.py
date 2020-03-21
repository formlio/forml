"""
Runtime unit tests fixtures.
"""
# pylint: disable=no-self-use
import collections
import datetime
import typing
import uuid

import pytest

from forml.project import distribution
from forml.runtime.asset import persistent, directory, access
from forml.runtime.asset.directory import lineage as lngmod, generation as genmod


@pytest.fixture(scope='function')
def nodes() -> typing.Sequence[uuid.UUID]:
    """Persistent nodes GID fixture.
    """
    return uuid.UUID(bytes=b'\x00' * 16), uuid.UUID(bytes=b'\x01' * 16), uuid.UUID(bytes=b'\x02' * 16)


@pytest.fixture(scope='function')
def states(nodes) -> typing.Mapping[uuid.UUID, bytes]:
    """State IDs to state values mapping fixture.
    """
    return collections.OrderedDict((n, n.bytes) for n in nodes)


@pytest.fixture(scope='function')
def tag(states: typing.Mapping[uuid.UUID, bytes]) -> genmod.Tag:
    """Tag fixture.
    """
    return genmod.Tag(training=genmod.Tag.Training(datetime.datetime(2019, 4, 1), 123),
                      tuning=genmod.Tag.Tuning(datetime.datetime(2019, 4, 5), 3.3),
                      states=states.keys())


@pytest.fixture(scope='session')
def populated_lineage(project_lineage: lngmod.Version) -> lngmod.Version:
    """Lineage fixture.
    """
    return project_lineage


@pytest.fixture(scope='session')
def empty_lineage() -> lngmod.Version:
    """Lineage fixture.
    """
    return lngmod.Version('2')


@pytest.fixture(scope='session')
def last_lineage(empty_lineage: lngmod.Version) -> lngmod.Version:
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
def registry(project_name: str, populated_lineage: lngmod.Version, empty_lineage: lngmod.Version,
             valid_generation: int, tag: genmod.Tag,
             states: typing.Mapping[uuid.UUID, bytes], project_package: distribution.Package) -> persistent.Registry:
    """Registry fixture.
    """
    content = {project_name: {populated_lineage: (project_package,
                                                  {valid_generation: (tag, tuple(states.values()))}),
                              empty_lineage: (project_package, {})}
               }

    class Registry(persistent.Registry):
        """Fixture registry implementation
        """
        def projects(self) -> typing.Iterable[str]:
            return content.keys()

        def lineages(self, project: str) -> typing.Iterable[lngmod.Version]:
            return content[project].keys()

        def generations(self, project: str, lineage: lngmod.Version) -> typing.Iterable[int]:
            try:
                return content[project][lineage][1].keys()
            except KeyError:
                raise directory.Level.Invalid(f'Invalid lineage ({lineage})')

        def pull(self, project: str, lineage: lngmod.Version) -> distribution.Package:
            return content[project][lineage][0]

        def push(self, package: distribution.Package) -> None:
            raise NotImplementedError()

        def read(self, project: str, lineage: lngmod.Version, generation: int, sid: uuid.UUID) -> bytes:
            if sid not in content[project][lineage][1][generation][0].states:
                raise directory.Level.Invalid(f'Invalid state id ({sid})')
            idx = content[project][lineage][1][generation][0].states.index(sid)
            return content[project][lineage][1][generation][1][idx]

        def write(self, project: str, lineage: lngmod.Version, sid: uuid.UUID, state: bytes) -> None:
            raise NotImplementedError()

        def open(self, project: str, lineage: lngmod.Version, generation: int) -> genmod.Tag:
            try:
                return content[project][lineage][1][generation][0]
            except KeyError:
                raise directory.Level.Invalid(f'Invalid generation ({lineage}.{generation})')

        def close(self, project: str, lineage: lngmod.Version, generation: int,
                  tag: genmod.Tag) -> None:
            raise NotImplementedError()

    return Registry()


@pytest.fixture(scope='function')
def valid_assets(project_name: str, populated_lineage: lngmod.Version, valid_generation: int,
                 registry: persistent.Registry) -> access.Assets:
    """Lineage fixture.
    """
    return access.Assets(project_name, populated_lineage, valid_generation, registry)
