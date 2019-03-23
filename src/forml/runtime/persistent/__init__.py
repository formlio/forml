import abc
import collections
import logging
import typing
import uuid

from forml import project as prjmod, runtime
from forml.runtime import resource

LOGGER = logging.getLogger(__name__)


class Generation:
    """Snapshot of project states in its particular training iteration.
    """
    def __init__(self, record: resource.Record, reader: typing.Callable[[uuid.UUID], bytes]):
        self.record: resource.Record = record
        self._reader: typing.Callable[[uuid.UUID], bytes] = reader

    def get(self, sid: uuid.UUID) -> bytes:
        """Get the state for given sid.

        Args:
            sid: State id

        Returns: State value as bytes.
        """
        return self._reader(sid)


class Lineage:
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, artifact: prjmod.Artifact,
                 getter: typing.Callable[[typing.Optional[int]], Generation],
                 writer: typing.Callable[[bytes], uuid.UUID],
                 closer: typing.Callable[[resource.Record], int]):
        self.artifact: prjmod.Artifact = artifact
        self._getter: typing.Callable[[typing.Optional[int]], Generation] = getter
        self._writer: typing.Callable[[bytes], uuid.UUID] = writer
        self._closer: typing.Callable[[resource.Record], int] = closer

    def get(self, generation: typing.Optional[int] = None) -> Generation:
        return self._getter(generation)

    def put(self, meta: resource.Record) -> Generation:
        return self._getter(self._closer(meta))

    def add(self, state: bytes) -> uuid.UUID:
        return self._writer(state)


class Registry(metaclass=abc.ABCMeta):
    """must be serializable.
    """
    STEP = 1

    class Empty(runtime.Error):
        """
        """

    def get(self, project: str, lineage: typing.Optional[int] = None) -> Lineage:
        def getter(generation: typing.Optional[int]) -> Generation:
            def reader(sid: uuid.UUID) -> bytes:
                if sid not in record.states:
                    raise KeyError('Unknown key: %s', sid)
                return self._read(sid)

            if not generation:
                generation = self._last(self._generations(project, lineage))
            record = self._open(project, lineage, generation)
            return Generation(record, reader)

        def writer(state: bytes) -> uuid.UUID:
            sid = uuid.uuid4()
            self._write(sid, state)
            return sid
        
        def closer(record: resource.Record) -> int:
            generation = self._next(self._generations(project, lineage))
            self._close(project, lineage, generation, record)
            return generation

        if not lineage:
            lineage = self._last(self._lineages(project))

        return Lineage(self._pull(project, lineage), getter, writer, closer)

    def put(self, project: str, artifact: prjmod.Artifact) -> Lineage:
        lineage = self._next(self._lineages(project))
        self._push(project, lineage, artifact)
        return self.get(project, lineage)

    @classmethod
    def _last(cls, listing: typing.Iterable[int]) -> int:
        try:
            return max(listing)
        except ValueError:
            raise cls.Empty('Empty listing')

    @classmethod
    def _next(cls, listing: typing.Iterable[int]) -> int:
        try:
            return cls._last(listing) + cls.STEP
        except cls.Empty:
            return cls.STEP

    @abc.abstractmethod
    def _lineages(self, project: str) -> typing.Sequence[int]:
        """
        Returns:
        """

    @abc.abstractmethod
    def _generations(self, project: str, lineage: int) -> typing.Sequence[int]:
        """
        Args:
            project:
            lineage:

        Returns:

        """

    @abc.abstractmethod
    def _pull(self, project: str, lineage: int) -> prjmod.Artifact:
        ...

    @abc.abstractmethod
    def _push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        """

        Args:
            project:
            artifact:

        Returns: New lineage id.
        """

    @abc.abstractmethod
    def _read(self, sid: uuid.UUID) -> bytes:
        ...

    @abc.abstractmethod
    def _write(self, sid: uuid.UUID, state: bytes) -> None:
        """

        Find first not sealed generation with given component missing or create new empty one.

        Args:
            project:
            lineage:
            component:
            state:

        Returns: New id.
        """

    @abc.abstractmethod
    def _close(self, project: str, lineage: int, generation: int, meta: resource.Record) -> None:
        """
        Args:
            project:
            lineage:
            component:
            state:

        Returns: New generation id.
        """

    def _open(self, project: str, lineage: int, generation: int) -> resource.Record:



class Manager:
    def __init__(self, generation: int):
        self._lineage: Lineage = ...
        self._generation = collections.defaultdict(lambda: generation)

    def load(self, gid) -> bytes:
        return self._lineage.get(self._generation[gid]).get(gid)

    def save(self, gid, state) -> Checksum:
        self._generation[gid] += 1

    def commit(self, *checksums: Checksum) -> None: