"""
Runtime persistence.
"""
import abc
import logging
import typing
import uuid

from forml import project as prjmod, runtime
from forml.runtime import resource

LOGGER = logging.getLogger(__name__)


class Generation:
    """Snapshot of project states in its particular training iteration.
    """
    def __init__(self, record: resource.Record):
        self.record: resource.Record = record


class Lineage:
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, artifact: prjmod.Artifact,
                 getter: typing.Callable[[typing.Optional[int]], Generation],
                 putter: typing.Callable[[resource.Record], int]):
        self.artifact: prjmod.Artifact = artifact
        self._getter: typing.Callable[[typing.Optional[int]], Generation] = getter
        self._putter: typing.Callable[[resource.Record], int] = putter

    def get(self, generation: typing.Optional[int] = None) -> Generation:
        """Get a generation instance by its id.

        Args:
            generation: Integer generation id.

        Returns: Generation instance.
        """
        return self._getter(generation)

    def put(self, record: resource.Record) -> Generation:
        """Commit a new generation described by its record. All states listed on the record are expected to have been
        provided previously by individual dumps.

        Args:
            record: Generation metadata.

        Returns: Generation instance.
        """
        return self._getter(self._putter(record))


class Registry(metaclass=abc.ABCMeta):
    """must be serializable.
    """
    class Listing:
        """Helper class representing a registry listing.
        """
        STEP = 1

        class Empty(runtime.Error):
            """Exception indicating empty listing.
            """

        def __init__(self, items: typing.Iterable[int], step: int = STEP):
            self._items: typing.Iterable[int] = items
            self._step = step

        @property
        def last(self) -> int:
            """Get the last (most recent) item from the listing.

            Returns: Id of the last item.
            """
            try:
                return max(self._items)
            except ValueError:
                raise self.Empty('Empty listing')

        @property
        def next(self) -> int:
            """Get the first item not in the listing bigger then any existing item.

            Returns: Id of the next item.
            """
            try:
                return self.last + self._step
            except self.Empty:
                return self._step

    def __str__(self):
        return f'{self.__class__.__name__}'

    def get(self, project: str, lineage: typing.Optional[int] = None) -> Lineage:
        """Get a lineage of given project.

        Args:
            project: Name of the project to work with.
            lineage: Optional lineage id to be loaded (defaults to the most recent id).

        Returns: Lineage instance.
        """
        def getter(generation: typing.Optional[int]) -> Generation:
            if not generation:
                generation = self._generations(project, lineage).last
            record = self._open(project, lineage, generation)
            return Generation(record)

        def closer(record: resource.Record) -> int:
            # TODO: verify all states exist
            generation = self._generations(project, lineage).next
            self._close(project, lineage, generation, record)
            return generation

        if not lineage:
            lineage = self._lineages(project).last

        return Lineage(self._pull(project, lineage), getter, closer)

    def put(self, project: str, artifact: prjmod.Artifact) -> Lineage:
        """Publish new lineage to the repository based on provided artifact.

        Args:
            project: Project the lineage belongs to. # TODO: project should be extracted from the artifact.
            artifact: Artifact to be published.

        Returns: new lineage instance based on the artifact.
        """
        lineage = self._lineages(project).next
        self._push(project, lineage, artifact)
        return self.get(project, lineage)

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an unbound state (not belonging to any project) under given state id.

        An unbound state is expected to be committed later into a new generation of specific lineage.

        Args:
            state: Serialized state to be persisted.

        Returns: Associated state id.
        """
        sid = uuid.uuid4()
        LOGGER.debug('%s: Dumping state %s', self, sid)
        self._write(sid, state)
        return sid

    def load(self, sid: uuid.UUID) -> bytes:
        """Load the state based on provided id.

        Args:
            sid: Id of the state object to be loaded.

        Returns: Serialized state.
        """
        LOGGER.debug('%s: Loading state %s', self, sid)
        return self._read(sid)

    @abc.abstractmethod
    def _lineages(self, project: str) -> 'Registry.Listing':
        """List the lineages of given project.

        Args:
            project: Project to be listed.

        Returns: Lineages listing.
        """

    @abc.abstractmethod
    def _generations(self, project: str, lineage: int) -> 'Registry.Listing':
        """List the generations of given lineage.

        Args:
            project: Project of which the lineage is to be listed.
            lineage: Lineage of the project to be listed.

        Returns: Generations listing.
        """

    @abc.abstractmethod
    def _pull(self, project: str, lineage: int) -> prjmod.Artifact:
        """Return the artifact of given lineage.

        Args:
            project: Project of which the lineage artifact is to be returned.
            lineage: Lineage of the project to return the artifact of.

        Returns: Project artifact object.
        """

    @abc.abstractmethod
    def _push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        """Start new lineage of a project based on given artifact.

        Args:
            project: Project to start the new lineage in.
            artifact: Artifact of the new lineage.
        """

    @abc.abstractmethod
    def _read(self, sid: uuid.UUID) -> bytes:
        """Load the state based on provided id.

        Args:
            sid: Id of the state object to be loaded.

        Returns: Serialized state.
        """

    @abc.abstractmethod
    def _write(self, sid: uuid.UUID, state: bytes) -> None:
        """Dump an unbound state under given state id.

        Args:
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """

    def _open(self, project: str, lineage: int, generation: int) -> resource.Record:
        """Return the metadata record of given generation.

        Args:
            project:
            lineage:
            generation:

        Returns: Generation metadata.
        """

    @abc.abstractmethod
    def _close(self, project: str, lineage: int, generation: int, record: resource.Record) -> None:
        """Seal new generation by storing its metadata record.

        Args:
            project: Project to store the metadata into.
            lineage: Lineage of the project to store the metadata into.
            generation: Generation of the project to store the metadata into.
            record: Generation metadata to be stored.
        """


class Assets:
    """Persistent assets IO for loading and dumping models.
    """
    def __init__(self, registry: Registry, project: str, lineage: typing.Optional[int] = None,
                 generation: typing.Optional[int] = None):
        self._registry: Registry = registry
        self._lineage: Lineage = registry.get(project, lineage)
        self._generation: typing.Optional[int]

    def load(self, sid: uuid.UUID) -> bytes:
        return self._registry.load(sid)

    def dump(self, state: bytes) -> uuid.UUID:
        return self._registry.dump(state)

    def commit(self, record: resource.Record) -> None:
        self._lineage.put(record)
