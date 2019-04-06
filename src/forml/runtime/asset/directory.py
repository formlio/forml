import abc
import collections
import datetime
import logging
import operator
import types
import typing
import uuid

from forml import project as prjmod, etl
from forml.runtime import asset

LOGGER = logging.getLogger(__name__)


class Error(asset.Error):
    """Asset error.
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

    def __init__(self, registry: asset.Registry, project: str, key: typing.Optional[int] = None,
                 parent: typing.Optional['Level'] = None):
        self._registry: asset.Registry = registry
        self.project: str = project
        self._key: typing.Optional[int] = key
        self._parent: typing.Optional[Level] = parent

    def __str__(self):
        return f'{self.project} ({".".join(str(v) for v in self.version)})'

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


class Generation(Level):
    """Snapshot of project states in its particular training iteration.
    """
    class Tag(typing.NamedTuple):
        """Generation metadata.
        """
        class Mode(types.SimpleNamespace):
            """Mode metadata.
            """
            class Proxy(tuple):
                """Mode attributes proxy.
                """
                _tag = property(operator.itemgetter(0))
                _mode = property(operator.itemgetter(1))

                def __new__(cls, tag: 'Generation.Tag', mode: 'Generation.Tag.Mode'):
                    return super().__new__(cls, (tag, mode))

                def __repr__(self):
                    return f'Mode{repr(self._mode)}'

                def __getattr__(self, item):
                    return getattr(self._mode, item)

                def __call__(self, **kwargs) -> 'Generation.Tag':
                    """Mode attributes setter.

                    Args:
                        **kwargs: Keyword parameters to be set on given mode attributes.

                    Returns: New tag instance with new values.
                    """
                    return Generation.Tag(**{k: self._mode.__class__(**{**self._mode.__dict__, **kwargs})
                                             if v is self._mode else v for k, v in self._tag._asdict().items()})

                @property
                def triggered(self) -> 'Generation.Tag':
                    """Create new tag with given mode triggered (all attributes reset and timestamp set to now).

                    Returns: New tag.
                    """
                    return self(timestamp=datetime.datetime.utcnow())

            def __init__(self, timestamp: typing.Optional[datetime.datetime], **kwargs: typing.Any):
                super().__init__(timestamp=timestamp, **kwargs)

        class Training(Mode):
            """Training mode attributes.
            """
            def __init__(self, timestamp: typing.Optional[datetime.datetime] = None,
                         ordinal: typing.Optional[etl.OrdinalT] = None):
                super().__init__(timestamp, ordinal=ordinal)

        class Tuning(Mode):
            """Tuning mode attributes.
            """
            def __init__(self, timestamp: typing.Optional[datetime.datetime] = None,
                         score: typing.Optional[float] = None):
                super().__init__(timestamp, score=score)

        training: Training = Training()
        tuning: Tuning = Tuning()
        states: typing.Iterable[uuid.UUID] = tuple()

        def __getattribute__(self, name: str) -> typing.Any:
            attribute = super().__getattribute__(name)
            if isinstance(attribute, Generation.Tag.Mode):
                attribute = self.Mode.Proxy(self, attribute)
            return attribute

        def replace(self, **kwargs) -> 'Generation.Tag':
            """Replace give non-mode attributes.

            Args:
                **kwargs: Non-mode attributes to be replaced.

            Returns: New tag instance.
            """
            if not {k for k, v in self._asdict().items()
                    if not isinstance(v, Generation.Tag.Mode)}.issuperset(kwargs.keys()):
                raise ValueError('Invalid replacement')
            return self._replace(**kwargs)

    def __init__(self, registry: asset.Registry, project: str, lineage: 'Lineage', key: typing.Optional[int] = None):
        super().__init__(registry, project, key, parent=lineage)
        self._tag: typing.Optional[Generation.Tag] = None

    @property
    def lineage(self) -> 'Lineage':
        """Get the lineage key of this generation.

        Returns: Lineage key of this generation.
        """
        return self._parent

    def _list(self) -> Level.Listing:
        """Return the listing of this level.

        Returns: Level listing.
        """
        return self._registry.generations(self.project, self.lineage.key)

    @property
    def tag(self) -> 'Generation.Tag':
        """Generation metadata.

        Returns: Metadata object.
        """
        if not self._tag:
            self._tag = self._registry.open(self.project, self.lineage.key, self.key)
        return self._tag

    def get(self, sid: typing.Union[uuid.UUID, int]) -> bytes:
        """Load the state based on provided id or positional index.

        Args:
            sid: Index or absolute id of the state object to be loaded.

        Returns: Serialized state.
        """
        if isinstance(sid, int):
            sid = self.tag.states[sid]
        if sid not in self.tag.states:
            raise ValueError(f'Unknown state reference for {self}: {sid}')
        LOGGER.debug('%s: Loading state %s', self, sid)
        return self._registry.read(self.project, self.lineage.key, self.key, sid)


class Lineage(Level):
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, registry: asset.Registry, project: str, key: typing.Optional[int] = None):
        super().__init__(registry, project, key)
        self._artifact: typing.Optional[prjmod.Artifact] = None

    @property
    def _list(self) -> Level.Listing:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self._registry.lineages(self.project)

    @property
    def artifact(self) -> prjmod.Artifact:
        """Lineage artifact.

        Returns: Artifact object.
        """
        if not self._artifact:
            self._artifact = self._registry.pull(self.project, self.key)
        return self._artifact

    def get(self, generation: typing.Optional[int] = None) -> Generation:
        """Get a generation instance by its id.

        Args:
            generation: Integer generation id.

        Returns: Generation instance.
        """
        return Generation(self._registry, self.project, self, generation)

    def put(self, tag: Generation.Tag) -> Generation:
        """Commit a new generation described by its tag. All states listed on the tag are expected to have
        been provided previously by individual dumps.

        Args:
            tag: Generation metadata.

        Returns: Generation instance.
        """
        # TODO: verify all states exist
        generation = self._list.next
        self._registry.close(self.project, self.key, generation, tag)
        return self.get(generation)

    def add(self, state: bytes) -> uuid.UUID:
        """Dump an unbound state (not belonging to any project) under given state id.

        An unbound state is expected to be committed later into a new generation of specific lineage.

        Args:
            state: Serialized state to be persisted.

        Returns: Associated state id.
        """
        sid = uuid.uuid4()
        LOGGER.debug('%s: Dumping state %s', self, sid)
        self._registry.write(self.project, self.key, sid, state)
        return sid
