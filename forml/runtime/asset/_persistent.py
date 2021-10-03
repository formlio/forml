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
import logging
import pathlib
import tempfile
import typing
import uuid

from forml import _provider, conf
from forml.conf.parsed import provider as provcfg  # pylint: disable=unused-import

if typing.TYPE_CHECKING:
    from forml import project as prj

    from ._directory import level

LOGGER = logging.getLogger(__name__)
TMPDIR = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
    prefix=f'{conf.APPNAME}-persistent-', dir=conf.tmpdir
)


def mkdtemp(prefix: typing.Optional[str] = None, suffix: typing.Optional[str] = None) -> pathlib.Path:
    """Custom temp-dir maker that puts all temps under our global temp.

    Args:
        prefix: Optional temp dir prefix.
        suffix: Optional temp dir suffix.

    Returns:
        Temp dir as pathlib path.
    """
    return pathlib.Path(tempfile.mkdtemp(prefix, suffix, TMPDIR.name))


class Registry(_provider.Interface, default=provcfg.Registry.default, path=provcfg.Registry.path):
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

    def mount(self, project: 'level.Project.Key', lineage: 'level.Lineage.Key') -> 'prj.Artifact':
        """Take given project/lineage package and return it as artifact instance.

        Args:
            project: Name of the project to work with.
            lineage: Lineage to be loaded.

        Returns:
            Product artifact.
        """
        package = self.pull(project, lineage)
        return package.install(self._staging / package.manifest.name / str(package.manifest.version))

    @abc.abstractmethod
    def projects(self) -> typing.Iterable[typing.Union[str, 'level.Project.Key']]:
        """List projects in given repository.

        Returns:
            Projects listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def lineages(self, project: 'level.Project.Key') -> typing.Iterable[typing.Union[str, 'level.Lineage.Key']]:
        """List the lineages of given prj.

        Args:
            project: Project to be listed.

        Returns:
            Lineages listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def generations(
        self, project: 'level.Project.Key', lineage: 'level.Lineage.Key'
    ) -> typing.Iterable[typing.Union[str, int, 'level.Generation.Key']]:
        """List the generations of given lineage.

        Args:
            project: Project of which the lineage is to be listed.
            lineage: Lineage of the project to be listed.

        Returns:
            Generations listing.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def pull(self, project: 'level.Project.Key', lineage: 'level.Lineage.Key') -> 'prj.Package':
        """Return the package of given lineage.

        Args:
            project: Project of which the lineage artifact is to be returned.
            lineage: Lineage of the project to return the artifact of.

        Returns:
            Project artifact object.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def push(self, package: 'prj.Package') -> None:
        """Start new lineage of a project based on given artifact.

        Args:
            package: Distribution package to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def read(
        self,
        project: 'level.Project.Key',
        lineage: 'level.Lineage.Key',
        generation: 'level.Generation.Key',
        sid: uuid.UUID,
    ) -> bytes:
        """Load the state based on provided id.

        Args:
            project: Project to read the state from.
            lineage: Lineage of the project to read the state from.
            generation: Generation of the project to read the state from.
            sid: Id of the state object to be loaded.

        Returns:
            Serialized state or empty byte-array if there is no such state for given (existing) generation.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def write(self, project: 'level.Project.Key', lineage: 'level.Lineage.Key', sid: uuid.UUID, state: bytes) -> None:
        """Dump an unbound state under given state id.

        Args:
            project: Project to store the state into.
            lineage: Lineage of the project to store the state into.
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def open(
        self, project: 'level.Project.Key', lineage: 'level.Lineage.Key', generation: 'level.Generation.Key'
    ) -> 'level.Tag':
        """Return the metadata tag of given generation.

        Args:
            project: Project to read the metadata from.
            lineage: Lineage of the project to read the metadata from.
            generation: Generation of the project to read the metadata from.

        Returns:
            Generation metadata.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def close(
        self,
        project: 'level.Project.Key',
        lineage: 'level.Lineage.Key',
        generation: 'level.Generation.Key',
        tag: 'level.Tag',
    ) -> None:
        """Seal new generation by storing its metadata tag.

        Args:
            project: Project to store the metadata into.
            lineage: Lineage of the project to store the metadata into.
            generation: Generation of the project to store the metadata into.
            tag: Generation metadata to be stored.
        """
        raise NotImplementedError()
