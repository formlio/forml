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

from forml import conf, error
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root

if typing.TYPE_CHECKING:
    from forml.project import product  # pylint: disable=unused-import
    from forml.runtime.asset.directory import project as prjmod, lineage as lngmod, generation as genmod  # noqa: F401

LOGGER = logging.getLogger(__name__)


class State:
    """State persistence accessor."""

    def __init__(
        self, generation: 'genmod.Level', nodes: typing.Sequence[uuid.UUID], tag: typing.Optional['genmod.Tag'] = None
    ):
        self._generation: 'genmod.Level' = generation
        self._nodes: typing.Tuple[uuid.UUID] = tuple(nodes)
        self._tag: typing.Optional['genmod.Tag'] = tag

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
            raise error.Unexpected(f'Unknown node ({gid})')
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
        return self._generation.lineage.dump(state)

    def commit(self, states: typing.Sequence[uuid.UUID]) -> None:
        """Create new generation by committing its previously dumped states.

        Args:
            states: Generation state IDs.
        """
        LOGGER.debug('Committing %d states %s', len(states), states)
        assert len(states) == len(self._nodes), 'Committed number of states not matching the number of nodes'
        tag = self._tag or self._generation.tag
        self._generation = self._generation.lineage.put(tag.replace(states=states))


class Assets:
    """Persistent assets IO for loading and dumping models."""

    def __init__(
        self,
        project: typing.Union[str, 'prjmod.Level.Key'] = conf.PRJNAME,
        lineage: typing.Optional[typing.Union[str, 'lngmod.Level.Key']] = None,
        generation: typing.Optional[typing.Union[str, int, 'genmod.Level.Key']] = None,
        registry: typing.Optional['root.Level'] = None,
    ):
        if not registry:
            registry = root.Level(persistent.Registry())
        self._generation: 'genmod.Level' = registry.get(project).get(lineage).get(generation)

    @property
    def project(self) -> 'product.Descriptor':
        """Get the project descriptor.

        Returns:
            Project descriptor.
        """
        return self._generation.lineage.artifact.descriptor

    @property
    def tag(self) -> 'genmod.Tag':
        """Get the generation tag.

        Returns:
            Generation tag.
        """
        return self._generation.tag

    def state(self, nodes: typing.Sequence[uuid.UUID], tag: typing.Optional['genmod.Tag'] = None) -> State:
        """Get the state persistence accessor wrapped in a context manager.

        Args:
            nodes: List of expected persisted stateful nodes.
            tag: Optional generation tag template to be used when committing.

        Returns:
            State persistence.
        """
        return State(self._generation, nodes, tag)
