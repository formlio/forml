"""
Runtime persistence.
"""
import abc
import collections
import logging
import typing
import uuid

from forml import project as prjmod, runtime
from forml.runtime import resource

LOGGER = logging.getLogger(__name__)


class Error(runtime.Error):
    """Persistence error.
    """


class Level(metaclass=abc.ABCMeta):
    class Listing:
        """Helper class representing a registry listing.
        """
        STEP = 1

        class Empty(Error):
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

    def __init__(self, registry: 'Registry', project: str, key: typing.Optional[int] = None,
                 parent: typing.Optional['Level'] = None):
        self._registry: Registry = registry
        self.project: str = project
        self._key: typing.Optional[int] = key
        self._parent: typing.Optional[Level] = parent

    def __str__(self):
        return f'{self.project}\t{".".join(str(v) for v in self.version)}'

    @property
    def version(self) -> typing.Sequence[int]:
        """Get the hierarchical version numbers.

        Returns: Version numbers.
        """
        def inspect(level: Level) -> typing.List[int]:
            """Get the version numbers of given levels parent tree.

            Args:
                level: Leaf level of the parent tree to scan through.

            Returns: List of parent tree levels.
            """
            version = inspect(level._parent) if level._parent else list()
            version.append(self.key)
            return version
        return tuple(inspect(self))

    @property
    def key(self) -> int:
        """Level key.

        Returns: ID of this level.
        """
        if self._key is None:
            self._key = self._list.last
        return self._key

    @property
    @abc.abstractmethod
    def _list(self) -> 'Level.Listing':
        """Return the listing of this level.

        Returns: Level listing.
        """


class Registry(metaclass=abc.ABCMeta):
    """Top-level persistent registry abstraction.
    """
    class Lineage(Level):
        """Sequence of generations based on same project artifact.
        """
        class Generation(Level):
            """Snapshot of project states in its particular training iteration.
            """
            def __init__(self, registry: 'Registry', project: str, lineage: int, key: typing.Optional[int] = None,
                         parent: typing.Optional['Level'] = None):
                super().__init__(registry, project, key, parent)
                self.lineage: int = lineage

            def _list(self) -> 'Level.Listing':
                """Return the listing of this level.

                Returns: Level listing.
                """
                return self._registry._generations(self.project, self.lineage)

            @property
            def record(self) -> resource.Record:
                """Generation metadata.

                Returns: Metadata object.
                """
                return self._registry._open(self.project, self.lineage, self.key)

        def __init__(self, registry: 'Registry', project: str, key: typing.Optional[int] = None):
            super().__init__(registry, project, key)

        @property
        def _list(self) -> 'Level.Listing':
            """List the content of this level.

            Returns: Level content listing.
            """
            return self._registry._lineages(self.project)

        @property
        def artifact(self) -> prjmod.Artifact:
            """Lineage artifact.

            Returns: Artifact object.
            """
            return self._registry._pull(self.project, self.key)

        def get(self, generation: typing.Optional[int] = None) -> Generation:
            """Get a generation instance by its id.

            Args:
                generation: Integer generation id.

            Returns: Generation instance.
            """
            return self.Generation(self._registry, self.project, self.key, generation, self)

        def put(self, record: resource.Record) -> Generation:
            """Commit a new generation described by its record. All states listed on the record are expected to have
            been provided previously by individual dumps.

            Args:
                record: Generation metadata.

            Returns: Generation instance.
            """
            # TODO: verify all states exist
            generation = self._list.next
            self._registry._close(self.project, self.key, generation, record)
            return self.get(generation)

        def dump(self, state: bytes) -> uuid.UUID:
            """Dump an unbound state (not belonging to any project) under given state id.

            An unbound state is expected to be committed later into a new generation of specific lineage.

            Args:
                state: Serialized state to be persisted.

            Returns: Associated state id.
            """
            sid = uuid.uuid4()
            LOGGER.debug('%s: Dumping state %s', self, sid)
            self._registry._write(self.project, self.key, sid, state)
            return sid

        def load(self, sid: uuid.UUID) -> bytes:
            """Load the state based on provided id.

            Args:
                sid: Id of the state object to be loaded.

            Returns: Serialized state.
            """
            LOGGER.debug('%s: Loading state %s', self, sid)
            return self._registry._read(self.project, self.key, sid)

    def __str__(self):
        return f'{self.__class__.__name__}-registry'

    def get(self, project: str, lineage: typing.Optional[int] = None) -> Lineage:
        """Get a lineage of given project.

        Args:
            project: Name of the project to work with.
            lineage: Optional lineage id to be loaded (defaults to the most recent id).

        Returns: Lineage instance.
        """
        return self.Lineage(self, project, lineage)

    def put(self, project: str, artifact: prjmod.Artifact) -> Lineage:
        """Publish new lineage to the repository based on provided artifact.

        Args:
            project: Project the lineage belongs to.
            artifact: Artifact to be published.

        Returns: new lineage instance based on the artifact.
        """
        lineage = self._lineages(project).next
        self._push(project, lineage, artifact)
        return self.get(project, lineage)

    @abc.abstractmethod
    def _lineages(self, project: str) -> 'Level.Listing':
        """List the lineages of given project.

        Args:
            project: Project to be listed.

        Returns: Lineages listing.
        """

    @abc.abstractmethod
    def _generations(self, project: str, lineage: int) -> 'Level.Listing':
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
    def _read(self, project: str, lineage: int, sid: uuid.UUID) -> bytes:
        """Load the state based on provided id.

        Args:
            project: Project to read the state from.
            lineage: Lineage of the project to read the state from.
            sid: Id of the state object to be loaded.

        Returns: Serialized state.
        """

    @abc.abstractmethod
    def _write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        """Dump an unbound state under given state id.

        Args:
            project: Project to store the state into.
            lineage: Lineage of the project to store the state into.
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """

    def _open(self, project: str, lineage: int, generation: int) -> resource.Record:
        """Return the metadata record of given generation.

        Args:
            project: Project to read the metadata from.
            lineage: Lineage of the project to read the metadata from.
            generation: Generation of the project to read the metadata from.

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
    def __init__(self, index: typing.Iterable[uuid.UUID], registry: Registry, project: str,
                 lineage: typing.Optional[int] = None, generation: typing.Optional[int] = None):
        self._index: collections.OrderedDict[
            uuid.UUID, typing.Optional[uuid.UUID]] = collections.OrderedDict((i, None) for i in index)
        self._lineage: Registry.Lineage = registry.get(project, lineage)
        self._generation: Registry.Lineage.Generation = self._lineage.get(generation)

    def index(self, sid: uuid.UUID) -> int:
        """Return the positional index of given relative state id.

        Args:
            sid: Searched relative state id.

        Returns: Positional index of give state id.
        """
        if sid not in self._index:
            raise ValueError(f'Unknown relative state ID ({sid})')
        return next(i for i, k in enumerate(self._index.keys()) if k == sid)

    def _rebind(self) -> None:
        """Bind the internal index of relative state IDs to the real IDs of current generation states.
        """
        LOGGER.debug('(Re)binding assets state IDs')
        record = self._generation.record
        if len(record.states) != len(self._index):
            raise Error('Persisted states cardinality mismatch')
        for key, value in zip(self._index, record.states):
            if not value:
                raise Error('Invalid absolute state ID binding attempt')
            self._index[key] = value

    def load(self, sid: uuid.UUID) -> bytes:
        """Load the state bound currently to given relative state ID.

        Args:
            sid: Relative state ID.

        Returns: Serialized state.
        """
        if sid not in self._index:
            raise ValueError(f'Unknown relative state ID ({sid})')
        if not self._index[sid]:
            self._rebind()
        return self._lineage.load(self._index[sid])

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an anonymous state to the repository.

        Args:
            state: State to be dumped.

        Returns: Associated absolute state ID.
        """
        return self._lineage.dump(state)

    def commit(self, record: resource.Record) -> None:
        """Create new generation by committing its previously dumped states referred in provided record.

        Args:
            record: Generation metadata.
        """
        self._generation = self._lineage.put(record)
        self._rebind()
