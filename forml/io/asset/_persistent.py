# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
ForML assets persistence.
"""
import abc
import atexit
import logging
import pathlib
import shutil
import tempfile
import typing
import uuid

import forml
from forml import conf, provider
from forml.conf.parsed import provider as provcfg  # pylint: disable=unused-import

if typing.TYPE_CHECKING:
    from forml import project as prj

    from ._directory import level

LOGGER = logging.getLogger(__name__)
TMPDIR = tempfile.mkdtemp(prefix=f'{conf.APPNAME}-persistent-', dir=conf.tmpdir)
atexit.register(lambda: shutil.rmtree(TMPDIR, ignore_errors=True))


def mkdtemp(prefix: typing.Optional[str] = None, suffix: typing.Optional[str] = None) -> pathlib.Path:
    """Custom temp-dir maker that puts all temps under our global temp.

    Args:
        prefix: Optional temp dir prefix.
        suffix: Optional temp dir suffix.

    Returns:
        Temp dir as pathlib path.
    """
    return pathlib.Path(tempfile.mkdtemp(prefix, suffix, TMPDIR))


class Registry(provider.Service, default=provcfg.Registry.default, path=provcfg.Registry.path):
    """Top-level persistent registry abstraction."""

    def __init__(self, staging: typing.Optional[typing.Union[str, pathlib.Path]] = None):
        if not staging:
            LOGGER.warning('Using temporal non-distributed staging for %s', self)
            staging = mkdtemp(prefix=f'{self}-staging-')
        self._staging: pathlib.Path = pathlib.Path(staging)

    def __repr__(self):
        name = self.__class__.__module__.rsplit('.', 1)[-1].capitalize()
        return f'{name}-registry'

    def __hash__(self):
        return hash(self.__class__) ^ hash(self._staging)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other._staging == self._staging

    def mount(self, project: 'level.Project.Key', release: 'level.Release.Key') -> 'prj.Artifact':
        """Take given project/release package and return it as artifact instance.

        Args:
            project: Name of the project to work with.
            release: Release to be loaded.

        Returns:
            Product artifact.
        """
        package = self.pull(project, release)
        try:
            return package.install(self._staging / package.manifest.name / str(package.manifest.version))
        except FileNotFoundError as err:
            raise forml.MissingError(f'Package artifact {project}-{release} not found') from err

    @abc.abstractmethod
    def projects(self) -> typing.Iterable[typing.Union[str, 'level.Project.Key']]:
        """List projects in given repository.

        Returns:
            Projects listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def releases(self, project: 'level.Project.Key') -> typing.Iterable[typing.Union[str, 'level.Release.Key']]:
        """List the releases of given existing project.

        Args:
            project: Existing project to be listed.

        Returns:
            Releases listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def generations(
        self, project: 'level.Project.Key', release: 'level.Release.Key'
    ) -> typing.Iterable[typing.Union[str, int, 'level.Generation.Key']]:
        """List the generations of given release.

        Args:
            project: Existing project of which the release is to be listed.
            release: Existing release of the project to be listed.

        Returns:
            Generations listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def pull(self, project: 'level.Project.Key', release: 'level.Release.Key') -> 'prj.Package':
        """Return the package of the given existing release.

        Args:
            project: Project of which the release artifact is to be returned.
            release: Release of the project to return the artifact of.

        Returns:
            Project artifact object.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def push(self, package: 'prj.Package') -> None:
        """Start new release of a (possibly new) project based on the given artifact.

        Args:
            package: Distribution package to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def read(
        self,
        project: 'level.Project.Key',
        release: 'level.Release.Key',
        generation: 'level.Generation.Key',
        sid: uuid.UUID,
    ) -> bytes:
        """Load the state from existing generation based on the provided id.

        Args:
            project: Project to read the state from.
            release: Release of the project to read the state from.
            generation: Generation of the project to read the state from.
            sid: Id of the state object to be loaded.

        Returns:
            Serialized state or empty byte-array if there is no such state for given (existing) generation.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def write(self, project: 'level.Project.Key', release: 'level.Release.Key', sid: uuid.UUID, state: bytes) -> None:
        """Dump a generation-unbound state within an existing release under the given state id.

        Args:
            project: Project to store the state into.
            release: Release of the project to store the state into.
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def open(
        self, project: 'level.Project.Key', release: 'level.Release.Key', generation: 'level.Generation.Key'
    ) -> 'level.Tag':
        """Return the metadata tag of the given existing generation.

        Args:
            project: Project to read the metadata from.
            release: Release of the project to read the metadata from.
            generation: Generation of the project to read the metadata from.

        Returns:
            Generation metadata.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def close(
        self,
        project: 'level.Project.Key',
        release: 'level.Release.Key',
        generation: 'level.Generation.Key',
        tag: 'level.Tag',
    ) -> None:
        """Seal a new - sofar unbound - generation within existing release by storing its metadata tag.

        Args:
            project: Project to store the metadata into.
            release: Release of the project to store the metadata into.
            generation: Generation of the project to store the metadata into.
            tag: Generation metadata to be stored.
        """
        raise NotImplementedError()


class Inventory(provider.Service, default=provcfg.Inventory.default, path=provcfg.Inventory.path):
    """Application descriptor storage abstraction."""

    def __repr__(self):
        name = self.__class__.__module__.rsplit('.', 1)[-1].capitalize()
        return f'{name}-inventory'

    @abc.abstractmethod
    def list(self) -> typing.Iterable[str]:
        """List the unique application names."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, application: str) -> 'prj.Descriptor':
        """Retrieve the descriptor for the given application.

        Only application returned by ``.list()`` can be requested.

        Args:
            application: Unique application name.

        Returns:
            Application descriptor.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def put(self, descriptor: 'prj.Descriptor.Handle') -> None:
        """Store the application descriptor into the inventory.

        Existing application with the same name gets overwritten.

        Args:
            descriptor: Application descriptor handle.
        """
        raise NotImplementedError()
