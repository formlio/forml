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
from forml import provider, setup

if typing.TYPE_CHECKING:
    from forml import application, project
    from forml.io import asset

LOGGER = logging.getLogger(__name__)
TMPDIR = tempfile.mkdtemp(prefix=f'{setup.APPNAME}-persistent-', dir=setup.tmpdir)
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


class Registry(provider.Service, default=setup.Registry.default, path=setup.Registry.path):
    """Abstract base class of the ForML model registry concept.

    Args:
        staging: File system location reachable from all runner nodes to be used for :ref:`package
                 staging <registry-staging>` (defaults to a local temporal directory (invalid for
                 distributed runners)).
    """

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

    @abc.abstractmethod
    def projects(self) -> typing.Iterable[typing.Union[str, 'asset.Project.Key']]:
        """List the existing projects contained in the repository.

        Returns:
            Projects listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def releases(self, project: 'asset.Project.Key') -> typing.Iterable[typing.Union[str, 'asset.Release.Key']]:
        """List the existing releases of the given *existing* project.

        Args:
            project: Existing project to be listed.

        Returns:
            Releases listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def generations(
        self, project: 'asset.Project.Key', release: 'asset.Release.Key'
    ) -> typing.Iterable[typing.Union[str, int, 'asset.Generation.Key']]:
        """List the existing generations of the given release.

        Args:
            project: Existing project whose release is to be listed.
            release: Existing project's release to be listed.

        Returns:
            Generations listing.
        """
        raise NotImplementedError()

    def mount(self, project: 'asset.Project.Key', release: 'asset.Release.Key') -> 'project.Artifact':
        """Pull and install the given project/release package using the *staging* file system
        location available to all runner nodes.

        Args:
            project: Name of the project to work with.
            release: Version of the release to be loaded.

        Returns:
            Product artifact.

        Raises:
            forml.MissingError: The given artifact could not be found.
        """
        package = self.pull(project, release)
        try:
            return package.install(self._staging / package.manifest.name / str(package.manifest.version))
        except FileNotFoundError as err:
            raise forml.MissingError(f'Package artifact {project}-{release} not found') from err

    @abc.abstractmethod
    def push(self, package: 'project.Package') -> None:
        """Start a new release of a (possibly new) project based on the given package artifact.

        Args:
            package: The release package to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def pull(self, project: 'asset.Project.Key', release: 'asset.Release.Key') -> 'project.Package':
        """Return the package of the given *existing* release.

        Args:
            project: Project of which the release artifact is to be returned.
            release: Project release to return the artifact of.

        Returns:
            Project artifact object.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def read(
        self,
        project: 'asset.Project.Key',
        release: 'asset.Release.Key',
        generation: 'asset.Generation.Key',
        sid: uuid.UUID,
    ) -> bytes:
        """Load the state from an *existing* generation based on the provided state ID.

        Args:
            project: Project to read the state from.
            release: Project release to read the state from.
            generation: Project generation to read the state from.
            sid: ID of the state object to be loaded.

        Returns:
            Serialized state or empty byte-array if there is no such state for the given (existing)
            generation.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def write(self, project: 'asset.Project.Key', release: 'asset.Release.Key', sid: uuid.UUID, state: bytes) -> None:
        """Dump a generation-unbound state within an *existing* release under the given state ID.

        Args:
            project: Project to store the state into.
            release: Project release to store the state into.
            sid: State ID to associate the payload with.
            state: Serialized state to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def open(
        self, project: 'asset.Project.Key', release: 'asset.Release.Key', generation: 'asset.Generation.Key'
    ) -> 'asset.Tag':
        """Return the metadata tag of the given *existing* generation.

        Args:
            project: Project to read the metadata from.
            release: Project release to read the metadata from.
            generation: Project generation to read the metadata from.

        Returns:
            Generation metadata.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def close(
        self,
        project: 'asset.Project.Key',
        release: 'asset.Release.Key',
        generation: 'asset.Generation.Key',
        tag: 'asset.Tag',
    ) -> None:
        """Commit a new - sofar unbound - generation within an *existing* release by storing its
        metadata tag.

        Args:
            project: Project to store the metadata into.
            release: Project release to store the metadata into.
            generation: Project generation to store the metadata into.
            tag: Generation metadata to be stored.
        """
        raise NotImplementedError()


class Inventory(provider.Service, default=setup.Inventory.default, path=setup.Inventory.path):
    """Abstract base class for application descriptor storage providers.

    Important:
        There is no concept of versioning - individual descriptors are held in a flat namespace
        requiring the uniqueness of each application :meth:`name
        <forml.application.Descriptor.name>`.
    """

    def __repr__(self):
        name = self.__class__.__module__.rsplit('.', 1)[-1].capitalize()
        return f'{name}-inventory'

    @abc.abstractmethod
    def list(self) -> typing.Iterable[str]:
        """List all the application names contained within the inventory.

        Returns:
            List of application names.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get(self, application: str) -> 'application.Descriptor':
        """Retrieve the descriptor for the given application name.

        Attention:
            Only applications returned by :meth:`list` can be requested.

        Args:
            application: Application :meth:`name <forml.application.Descriptor.name>`.

        Returns:
            Application descriptor.

        Raises:
            forml.MissingError: If the application does not exist.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def put(self, descriptor: 'application.Descriptor.Handle') -> None:
        """Store the application descriptor in the inventory.

        Caution:
            Existing application with the same :meth:`name <forml.application.Descriptor.name>`
            gets overwritten.

        Args:
            descriptor: Handle of the application descriptor to be stored.
        """
        raise NotImplementedError()
