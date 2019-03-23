import abc
import collections
import functools
import typing

from forml import project as prjmod


KeyT = typing.TypeVar('KeyT')


class Generation:
    class Meta(collections.namedtuple('Meta', 'training, tuning, checksums')):
        """
        """

    def __init__(self, loader: typing.Callable[[KeyT], bytes], reader: typing.Callable[[], Meta]):
        self._loader: typing.Callable[[KeyT], bytes] = loader
        self._reader: typing.Callable[[], Generation.Meta] = reader

    def get(self, state: KeyT) -> bytes:
        """
        Args:
            state:

        Returns:
        """
        return self._loader(state)

    @property
    def meta(self) -> Meta:
        return self._reader()


class Lineage:
    """
    """

    class Artifact(collections.namedtuple('Artifact', 'path')):

        @property
        def descriptor(self) -> prjmod.Descriptor:
            """
            forml.pyz (contains project code and all the deps)

            Returns: Project descriptor.
            """

    def __init__(self, puller: typing.Callable[[], Artifact],
                 getter: typing.Callable[[typing.Optional[int]], Generation],
                 dumper: typing.Callable[[bytes], KeyT],
                 writer: typing.Callable[[Generation.Meta], int]):
        self._puller: typing.Callable[[], Lineage.Artifact] = puller
        self._getter: typing.Callable[[typing.Optional[int]], Generation] = getter
        self._dumper: typing.Callable[[bytes], KeyT] = dumper
        self._writer: typing.Callable[[Generation.Meta], int] = writer

    def get(self, generation: typing.Optional[int] = None) -> Generation:
        return self._getter(generation)

    def put(self, meta: Generation.Meta) -> Generation:
        return self._getter(self._writer(meta))

    def add(self, state: bytes) -> KeyT:
        return self._dumper(state)

    @property
    def artifact(self) -> Artifact:
        return self._puller()


class Empty(Exception):
    """
    """


class Registry(typing.Generic[KeyT], metaclass=abc.ABCMeta):
    """must be serializable.
    """
    def get(self, project: str, lineage: typing.Optional[int] = None) -> Lineage:
        def getter(generation: typing.Optional[int]) -> Generation:
            if generation is None:
                listing = self._generations(project, lineage)
                if not listing:
                    raise Empty('No generations for linage %s of the project %s', lineage, project)
                generation = listing[-1]
            return Generation(self._load, functools.partial(self._read, project, lineage, generation))

        if lineage is None:
            listing = self._lineages(project)
            if not listing:
                raise Empty('No lineages for project %s', project)
            lineage = listing[-1]
        return Lineage(functools.partial(self._pull, project, lineage), getter, self._dump,
                       functools.partial(self._write, project, lineage))

    def put(self, project: str, artifact: Lineage.Artifact) -> Lineage:
        return self.get(project, self._push(project, artifact))

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
    def _pull(self, project: str, lineage: int) -> Lineage.Artifact:
        ...

    @abc.abstractmethod
    def _push(self, project: str, artifact: Lineage.Artifact) -> int:
        """

        Args:
            project:
            artifact:

        Returns: New lineage id.
        """

    @abc.abstractmethod
    def _load(self, state: KeyT) -> bytes:
        ...

    @abc.abstractmethod
    def _dump(self, state: bytes) -> KeyT:
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
    def _write(self, project: str, lineage: int, meta: Generation.Meta) -> int:
        """
        Args:
            project:
            lineage:
            component:
            state:

        Returns: New generation id.
        """

    def _read(self, project: str, lineage: int, generation: int) -> Generation.Meta:


class Manager:
    def __init__(self, generation: int):
        self._lineage: Lineage = ...
        self._generation = collections.defaultdict(lambda: generation)

    def load(self, gid) -> bytes:
        return self._lineage.get(self._generation[gid]).get(gid)

    def save(self, gid, state) -> Checksum:
        self._generation[gid] += 1

    def commit(self, *checksums: Checksum) -> None: