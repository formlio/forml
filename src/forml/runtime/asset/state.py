import typing
import uuid

from forml.runtime.asset import directory


class Manager:
    """State persistence accessor.
    """
    def __init__(self, generation: directory.Generation, tag: typing.Optional[directory.Generation.Tag] = None):
        self._generation: directory.Generation = generation
        self._tag: typing.Optional[directory.Generation.Tag] = tag

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
