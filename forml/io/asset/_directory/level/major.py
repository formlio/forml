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

"""Generic assets directory.
"""
import logging
import typing
import uuid

from packaging import version as vermod

from ... import _directory, _persistent
from . import minor as genmod

if typing.TYPE_CHECKING:
    from forml import project as prj

    from . import case as prjmod

LOGGER = logging.getLogger(__name__)
ARTIFACTS = _directory.Cache(_persistent.Registry.mount)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Release(_directory.Level):
    """Sequence of generations based on same project artifact."""

    class Key(_directory.Level.Key, vermod.Version):  # pylint: disable=abstract-method
        """Release key."""

        MIN = '0'

        def __init__(self, key: typing.Union[str, 'Release.Key'] = MIN):
            try:
                super().__init__(str(key))
            except vermod.InvalidVersion as err:
                raise self.Invalid(f'Invalid version {key} (not PEP 440 compliant)') from err

    def __init__(self, project: 'prjmod.Project', key: typing.Optional[typing.Union[str, 'Release.Key']] = None):
        super().__init__(key, parent=project)

    @property
    def project(self) -> 'prjmod.Project':
        """Get the project of this generation.

        Returns:
            Project of this generation.
        """
        return self._parent

    @property
    def artifact(self) -> 'prj.Artifact':
        """Release artifact.

        Returns:
            Artifact object.
        """
        return ARTIFACTS(self.registry, self.project.key, self.key)

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an unbound state (not belonging to any project) under given state id.

        An unbound state is expected to be committed later into a new generation of specific release.

        Args:
            state: Serialized state to be persisted.

        Returns:
            Associated state id.
        """
        sid = uuid.uuid4()
        LOGGER.debug('%s: Dumping state %s', self, sid)
        self.registry.write(self.project.key, self.key, sid, state)
        return sid

    def list(self) -> _directory.Level.Listing:
        """List the content of this level.

        Returns:
            Level content listing.
        """
        return self.Listing(genmod.Generation.Key(g) for g in self.registry.generations(self.project.key, self.key))

    def get(self, key: typing.Optional[typing.Union[str, int, genmod.Generation.Key]] = None) -> genmod.Generation:
        """Get a generation instance by its id.

        Args:
            key: Integer generation id.

        Returns:
            genmod.Level instance.
        """
        return genmod.Generation(self, key)

    def put(self, tag: genmod.Tag) -> genmod.Generation:
        """Commit a new generation described by its tag. All states listed on the tag are expected to have
        been provided previously by individual dumps.

        Args:
            tag: genmod.Level metadata.

        Returns:
            genmod.Level instance.
        """
        try:
            generation = self.list().last.next
        except self.Listing.Empty:
            generation = 1
        self.registry.close(self.project.key, self.key, generation, tag)
        return self.get(generation)
