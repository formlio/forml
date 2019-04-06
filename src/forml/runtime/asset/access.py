import typing
import uuid

import forml.runtime.asset.directory
from forml import project as prjmod
from forml.runtime.asset import persistent


class State:
    """State persistence accessor.
    """
    def __init__(self, generation: forml.runtime.asset.directory.Generation, tag: typing.Optional[
        forml.runtime.asset.directory.Generation.Tag] = None):
        self._generation: forml.runtime.asset.directory.Generation = generation
        self._tag: typing.Optional[forml.runtime.asset.directory.Generation.Tag] = tag

    def load(self, sid: typing.Union[int, uuid.UUID]) -> bytes:
        """Load the state based on its ordering index.

        Args:
            sid: State index as of the taged ordering.

        Returns: Serialized state.
        """
        return self._generation.get(sid)

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an anonymous state to the repository.

        Args:
            state: State to be dumped.

        Returns: Associated absolute state ID.
        """
        return self._generation.lineage.add(state)

    def commit(self, states: typing.Sequence[uuid.UUID]) -> None:
        """Create new generation by committing its previously dumped states.

        Args:
            states: Generation states.
        """
        tag = self._tag or self._generation.tag
        self._generation = self._generation.lineage.put(tag.replace(states=states))


class Assets:
    """Persistent assets IO for loading and dumping models.
    """
    def __init__(self, registry: persistent.Registry, project: str, lineage: typing.Optional[int] = None,
                 generation: typing.Optional[int] = None):
        self._generation: forml.runtime.asset.directory.Generation = registry.get(project, lineage).get(generation)

    @property
    def project(self) -> prjmod.Descriptor:
        """Get the project descriptor.

        Returns: Project descriptor.
        """
        return self._generation.lineage.artifact.descriptor

    @property
    def tag(self) -> forml.runtime.asset.directory.Generation.Tag:
        """Get the generation tag.

        Returns: Generation tag.
        """
        return self._generation.tag

    def state(self, tag: typing.Optional[forml.runtime.asset.directory.Generation.Tag] = None) -> State:
        """Get the state persistence accessor.

        Args:
            tag: Optional generation tag template to be used when committing.

        Returns: State persistence manager.
        """
        return State(self._generation, tag)
