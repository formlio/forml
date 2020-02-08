"""Generic assets directory.
"""
import abc
import collections
import datetime
import json
import logging
import operator
import types
import typing
import uuid

from forml import etl, project as prjmod  # pylint: disable=unused-import
from forml.runtime import asset
from forml.runtime.asset import persistent


LOGGER = logging.getLogger(__name__)


class Error(asset.Error):
    """Asset error.
    """


class Level(metaclass=abc.ABCMeta):
    """Abstract directory level.
    """
    class Invalid(Error):
        """Indication of an invalid level.
        """

    class Listing:
        """Helper class representing a registry listing.
        """
        STEP = 1

        class Empty(Error):
            """Exception indicating empty listing.
            """

        def __init__(self, items: typing.Collection[int], step: int = STEP):
            self._items: typing.Collection[int] = items
            self._step = step

        def __contains__(self, index: int) -> bool:
            return index in self._items

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

    def __init__(self, registry: 'persistent.Registry', project: str, key: typing.Optional[int] = None,
                 parent: typing.Optional['Level'] = None):
        self._registry: persistent.Registry = registry
        self.project: str = project
        self._key: typing.Optional[int] = key
        self._parent: typing.Optional[Level] = parent
        self._listing: typing.Optional[Level.Listing] = None

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
        """Either user specified or last listed level key.

        Returns: ID of this level.
        """
        if self._key is None:
            self._key = self.listing.last
        if self._key not in self.listing:
            raise Level.Invalid(f'Invalid level key {self._key}')
        return self._key

    @property
    def listing(self) -> 'Level.Listing':
        """Lazily cached level listing.

        Returns: Level listing.
        """
        if not self._listing:
            self._listing = self._list()
        return self._listing

    @abc.abstractmethod
    def _list(self) -> 'Level.Listing':
        """Return the listing of this level.

        Returns: Level listing.
        """


class Generation(Level):
    """Snapshot of project states in its particular training iteration.
    """
    class Tag(collections.namedtuple('Tag', 'training, tuning, states')):
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

                def __bool__(self):
                    return bool(self._mode)

                def __getattr__(self, item):
                    return getattr(self._mode, item)

                def __eq__(self, other) -> bool:
                    # pylint: disable=protected-access
                    return isinstance(other, self.__class__) and self._mode == other._mode

                def replace(self, **kwargs) -> 'Generation.Tag':
                    """Mode attributes setter.

                    Args:
                        **kwargs: Keyword parameters to be set on given mode attributes.

                    Returns: New tag instance with new values.
                    """
                    return Generation.Tag(**{k: self._mode.__class__(**{**self._mode.__dict__, **kwargs})
                                                if v is self._mode else v for k, v in self._tag._asdict().items()})

                def trigger(self, timestamp: typing.Optional[datetime.datetime] = None) -> 'Generation.Tag':
                    """Create new tag with given mode triggered (all attributes reset and timestamp set to now).

                    Returns: New tag.
                    """
                    return self.replace(timestamp=(timestamp or datetime.datetime.utcnow()))

            def __init__(self, timestamp: typing.Optional[datetime.datetime], **kwargs: typing.Any):
                super().__init__(timestamp=timestamp, **kwargs)

            def __bool__(self):
                return bool(self.timestamp)

        class Training(Mode):
            """Training mode attributes.
            """
            def __init__(self, timestamp: typing.Optional[datetime.datetime] = None,
                         ordinal: typing.Optional['etl.OrdinalT'] = None):
                super().__init__(timestamp, ordinal=ordinal)

        class Tuning(Mode):
            """Tuning mode attributes.
            """
            def __init__(self, timestamp: typing.Optional[datetime.datetime] = None,
                         score: typing.Optional[float] = None):
                super().__init__(timestamp, score=score)

        _TSFMT = '%Y-%m-%dT%H:%M:%S.%f'

        def __new__(cls, training: typing.Optional[Training] = None, tuning: typing.Optional[Tuning] = None,
                    states: typing.Optional[typing.Sequence[uuid.UUID]] = None):
            return super().__new__(cls, training or cls.Training(), tuning or cls.Tuning(), tuple(states or []))

        def __bool__(self):
            return bool(self.training or self.tuning)

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

        @classmethod
        def _strftime(cls, timestamp: typing.Optional[datetime.datetime]) -> typing.Optional[str]:
            """Encode the timestamp into string representation.

            Args:
                timestamp: Timestamp to be encoded.

            Returns: Timestamp string representation.
            """
            if not timestamp:
                return None
            return timestamp.strftime(cls._TSFMT)

        @classmethod
        def _strptime(cls, raw: typing.Optional[str]) -> typing.Optional[datetime.datetime]:
            """ Decode the timestamp from string representation.

            Args:
                raw: Timestamp string representation.

            Returns: Timestamp instance.
            """
            if not raw:
                return None
            return datetime.datetime.strptime(raw, cls._TSFMT)

        def dumps(self) -> bytes:
            """Dump the tag into a string of bytes.

            Returns: String of bytes representation.
            """
            return json.dumps({
                'training': {'timestamp': self._strftime(self.training.timestamp)},
                'tuning': {'timestamp': self._strftime(self.tuning.timestamp)},
                'states': [str(s) for s in self.states]}, indent=4).encode('utf-8')

        @classmethod
        def loads(cls, raw: bytes) -> 'Generation.Tag':
            """Loaded the dumped tag.

            Args:
                raw: Serialized tag representation to be loaded.

            Returns: Tag instance.
            """
            meta = json.loads(raw, encoding='utf-8')
            return cls(training=cls.Training(timestamp=cls._strptime(meta['training']['timestamp'])),
                       tuning=cls.Tuning(timestamp=cls._strptime(meta['tuning']['timestamp'])),
                       states=(uuid.UUID(s) for s in meta['states']))

    def __init__(self, registry: 'persistent.Registry', project: str, lineage: 'Lineage',
                 key: typing.Optional[int] = None):
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
        """Generation metadata. In case of implicit generation and empty lineage this returns a "null" tag (a Tag object
        with all fields empty)

        Returns: Generation tag (metadata) object.
        """
        if not self._tag:
            try:
                self._tag = self._registry.open(self.project, self.lineage.key, self.key)
            except self.Listing.Empty:
                LOGGER.warning('No previous generations found - using a null tag')
                self._tag = self.Tag()
        return self._tag

    def get(self, sid: typing.Union[uuid.UUID, int]) -> bytes:
        """Load the state based on provided id or positional index.

        Args:
            sid: Index or absolute id of the state object to be loaded.

        Returns: Serialized state.
        """
        if not self.tag.training:
            return bytes()
        if isinstance(sid, int):
            sid = self.tag.states[sid]
        if sid not in self.tag.states:
            raise ValueError(f'Unknown state reference for {self}: {sid}')
        LOGGER.debug('%s: Loading state %s', self, sid)
        return self._registry.read(self.project, self.lineage.key, self.key, sid)


class Lineage(Level):
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, registry: 'persistent.Registry', project: str, key: typing.Optional[int] = None):
        super().__init__(registry, project, key)
        self._artifact: typing.Optional[prjmod.Artifact] = None

    def _list(self) -> Level.Listing:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self._registry.lineages(self.project)

    @property
    def artifact(self) -> 'prjmod.Artifact':
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
        # TO DO: verify all states exist
        generation = self.listing.next
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
