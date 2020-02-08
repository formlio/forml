"""ForML assets accessing functionality.
"""
import contextlib
import logging
import typing
import uuid

from forml import conf, project as prjmod  # pylint: disable=unused-import
from forml.runtime.asset import directory
from forml.runtime.asset import persistent

LOGGER = logging.getLogger(__name__)


class State:
    """State persistence accessor.
    """
    def __init__(self, generation: directory.Generation, nodes: typing.Sequence[uuid.UUID],
                 tag: typing.Optional[directory.Generation.Tag] = None):
        self._generation: directory.Generation = generation
        self._nodes: typing.Tuple[uuid.UUID] = tuple(nodes)
        self._tag: typing.Optional[directory.Generation.Tag] = tag

    def __contains__(self, gid: uuid.UUID) -> bool:
        """Check whether given node is persistent (on our state list).

        Args:
            gid: Node gid to be tested.

        Returns: True if persistent.
        """
        return gid in self._nodes

    def offset(self, gid: uuid.UUID) -> int:
        """Get the offset of given node in the persistent node list.

        Args:
            gid: Id of node to be looked up for its offset.

        Returns: Offset of given node.
        """
        if gid not in self._nodes:
            raise ValueError(f'Unknown node ({gid})')
        return self._nodes.index(gid)

    def load(self, gid: uuid.UUID) -> bytes:
        """Load the state based on its state id, ordering index or node gid.

        Args:
            gid: Node group id.

        Returns: Serialized state.
        """
        LOGGER.debug('Loading state %s', gid)
        return self._generation.get(self.offset(gid))

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an anonymous state to the repository returning its associated state ID. Caller is expected to send that
        state ID under given offset to the commit.

        Args:
            state: State to be dumped.

        Returns: Associated absolute state ID.
        """
        LOGGER.debug('Dumping state (%d bytes)', len(state))
        return self._generation.lineage.add(state)

    def commit(self, states: typing.Sequence[uuid.UUID]) -> None:
        """Create new generation by committing its previously dumped states.

        Args:
            states: Generation state IDs.
        """
        LOGGER.debug('Committing %d states %s', len(states), states)
        assert len(states) == len(self._nodes), f'Committed number of states not matching the number of nodes'
        tag = self._tag or self._generation.tag
        self._generation = self._generation.lineage.put(tag.replace(states=states))


class Assets:
    """Persistent assets IO for loading and dumping models.
    """
    def __init__(self, project: str = conf.PRJ_NAME,
                 lineage: typing.Optional[int] = None, generation: typing.Optional[int] = None,
                 registry: persistent.Registry = persistent.Registry()):
        self._generation: directory.Generation = registry.get(project, lineage).get(generation)

    @property
    def project(self) -> 'prjmod.Descriptor':
        """Get the project descriptor.

        Returns: Project descriptor.
        """
        return self._generation.lineage.artifact.descriptor

    @property
    def tag(self) -> directory.Generation.Tag:
        """Get the generation tag.

        Returns: Generation tag.
        """
        return self._generation.tag

    def state(self, nodes: typing.Sequence[uuid.UUID],
              tag: typing.Optional[directory.Generation.Tag] = None) -> typing.ContextManager[State]:
        """Get the state persistence accessor wrapped in a context manager.

        Args:
            nodes: List of expected persisted stateful nodes.
            tag: Optional generation tag template to be used when committing.

        Returns: State persistence in a context manager.
        """
        @contextlib.contextmanager
        def context(state: State) -> typing.Generator[State, None, None]:
            """State context manager that updates the generation upon exit.
            """
            yield state
            self._generation = state._generation  # pylint: disable=protected-access

        return context(State(self._generation, nodes, tag))
