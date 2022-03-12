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

"""ForML assets accessing functionality.
"""
import logging
import typing
import uuid

import forml
from forml import conf

from . import _persistent
from ._directory import level

if typing.TYPE_CHECKING:
    from forml import project as prj

LOGGER = logging.getLogger(__name__)


class State:
    """State persistence accessor."""

    def __init__(
        self,
        generation: 'level.Generation',
        nodes: typing.Sequence[uuid.UUID],
        tag: typing.Optional['level.Tag'] = None,
    ):
        self._generation: 'level.Generation' = generation
        self._nodes: tuple[uuid.UUID] = tuple(nodes)
        self._tag: typing.Optional['level.Tag'] = tag

    def __contains__(self, gid: uuid.UUID) -> bool:
        """Check whether given node is persistent (on our state list).

        Args:
            gid: Node gid to be tested.

        Returns:
            True if persistent.
        """
        return gid in self._nodes

    def offset(self, gid: uuid.UUID) -> int:
        """Get the offset of given node in the persistent node list.

        Args:
            gid: Id of node to be looked up for its offset.

        Returns:
            Offset of given node.
        """
        if gid not in self._nodes:
            raise forml.UnexpectedError(f'Unknown node ({gid})')
        return self._nodes.index(gid)

    def load(self, gid: uuid.UUID) -> bytes:
        """Load the state based on its state id, ordering index or node gid.

        Args:
            gid: Node group id.

        Returns:
            Serialized state.
        """
        LOGGER.debug('Loading state %s', gid)
        return self._generation.get(self.offset(gid))

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an anonymous state to the repository returning its associated state ID. Caller is expected to send that
        state ID under given offset to the commit.

        Args:
            state: State to be dumped.

        Returns:
            Associated absolute state ID.
        """
        LOGGER.debug('Dumping state (%d bytes)', len(state))
        return self._generation.release.dump(state)

    def commit(self, states: typing.Sequence[uuid.UUID]) -> None:
        """Create new generation by committing its previously dumped states.

        Args:
            states: Generation state IDs.
        """
        LOGGER.debug('Committing %d states %s', len(states), states)
        assert len(states) == len(self._nodes), 'Committed number of states not matching the number of nodes'
        tag = self._tag or self._generation.tag
        self._generation = self._generation.release.put(tag.replace(states=states))


class Instance:
    """Persistent assets IO for loading and dumping models."""

    def __init__(
        self,
        project: typing.Union[str, 'level.Project.Key'] = conf.PRJNAME,
        release: typing.Optional[typing.Union[str, 'level.Release.Key']] = None,
        generation: typing.Optional[typing.Union[str, int, 'level.Generation.Key']] = None,
        registry: typing.Optional['level.Directory'] = None,
    ):
        if not registry:
            registry = level.Directory(_persistent.Registry())
        self._generation: 'level.Generation' = registry.get(project).get(release).get(generation)

    def __hash__(self):
        return hash(self._generation)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other._generation == self._generation

    @property
    def project(self) -> 'prj.Components':
        """Get the project components.

        Returns:
            Project components.
        """
        return self._generation.release.artifact.components

    @property
    def tag(self) -> 'level.Tag':
        """Get the generation tag.

        Returns:
            Generation tag.
        """
        return self._generation.tag

    def state(self, nodes: typing.Sequence[uuid.UUID], tag: typing.Optional['level.Tag'] = None) -> State:
        """Get the state persistence accessor.

        Args:
            nodes: List of expected persisted stateful nodes.
            tag: Optional generation tag template to be used when committing.

        Returns:
            State persistence.
        """
        return State(self._generation, nodes, tag)
