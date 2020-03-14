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
from collections import abc as colabc

from packaging import version

from forml import error  # pylint: disable=unused-import; # noqa: F401
from forml.runtime.asset import persistent

if typing.TYPE_CHECKING:
    from forml.project import product, distribution
    from forml import etl  # pylint: disable=unused-import; # noqa: F401

LOGGER = logging.getLogger(__name__)

KeyT = typing.TypeVar('KeyT', str, version.Version, int)
ItemT = typing.TypeVar('ItemT', version.Version, int)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Level(typing.Generic[KeyT, ItemT], metaclass=abc.ABCMeta):
    """Abstract directory level.
    """
    class Invalid(error.Invalid):
        """Indication of an invalid level.
        """

    class Listing(typing.Generic[ItemT], colabc.Iterable):
        """Helper class representing a registry listing.
        """
        class Empty(error.Missing):
            """Exception indicating empty listing.
            """

        def __init__(self, items: typing.Iterable[ItemT]):
            self._items: typing.Tuple[ItemT] = tuple(sorted(set(items)))

        def __contains__(self, key: ItemT) -> bool:
            return key in self._items

        def __eq__(self, other):
            return isinstance(other, self.__class__) and other._items == self._items

        def __iter__(self):
            return iter(self._items)

        def __str__(self):
            return ', '.join(str(i) for i in self._items)

        @property
        def last(self) -> ItemT:
            """Get the last (most recent) item from the listing.

            Returns: Id of the last item.
            """
            try:
                return self._items[-1]
            except IndexError:
                raise self.Empty('Empty listing')

    def __init__(self, registry: 'persistent.Registry', key: typing.Optional[KeyT] = None,
                 parent: typing.Optional['Level'] = None):
        self._key: typing.Optional[KeyT] = key
        self._parent: typing.Optional[Level] = parent
        self._registry: persistent.Registry = registry

    def __str__(self):
        return f'{self.__class__.__name__.capitalize()} {self.version}'

    @property
    def version(self) -> str:
        """Get the hierarchical version number.

        Returns: Version number.
        """
        return f'{self._parent}-{self.key}' if self._parent else self.key

    @property
    def key(self) -> KeyT:
        """Either user specified or last lazily listed level key.

        Returns: ID of this level.
        """
        if self._key is None:
            if not self._parent:
                raise ValueError('Parent or key required')
            self._key = self._parent.list().last
        if self._parent and self._key not in self._parent.list():
            raise Level.Invalid(f'Invalid level key {self._key}')
        return self._key

    @abc.abstractmethod
    def list(self) -> 'Level.Listing[ItemT]':
        """Return the listing of this level.

        Returns: Level listing.
        """

    @abc.abstractmethod
    def get(self, key: ItemT) -> 'Level[ItemT, typing.Any]':
        """Get an item from this level.

        Args:
            key: Item key to get.

        Returns: Item as a level instance.
        """


class Generation(Level[int, uuid.UUID]):
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
                    mode = self._mode.__class__(**{**self._mode.__dict__, **kwargs})
                    return Generation.Tag(**{k: mode if v is self._mode else v for k, v in self._tag._asdict().items()})

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
                'training': {'timestamp': self._strftime(self.training.timestamp), 'ordinal': self.training.ordinal},
                'tuning': {'timestamp': self._strftime(self.tuning.timestamp), 'score': self.tuning.score},
                'states': [str(s) for s in self.states]}, indent=4).encode('utf-8')

        @classmethod
        def loads(cls, raw: bytes) -> 'Generation.Tag':
            """Loaded the dumped tag.

            Args:
                raw: Serialized tag representation to be loaded.

            Returns: Tag instance.
            """
            meta = json.loads(raw, encoding='utf-8')
            return cls(training=cls.Training(timestamp=cls._strptime(meta['training']['timestamp']),
                                             ordinal=meta['training']['ordinal']),
                       tuning=cls.Tuning(timestamp=cls._strptime(meta['tuning']['timestamp']),
                                         score=meta['tuning']['score']),
                       states=(uuid.UUID(s) for s in meta['states']))

    NOTAG = Tag()

    def __init__(self, registry: 'persistent.Registry', lineage: 'Lineage', key: typing.Optional[int] = None):
        if key:
            key = int(key)
        super().__init__(registry, key, parent=lineage)
        self._tag: typing.Optional[Generation.Tag] = None

    @property
    def project(self) -> 'Project':
        """Get the project of this generation.

        Returns: Project of this generation.
        """
        return self.lineage.project

    @property
    def lineage(self) -> 'Lineage':
        """Get the lineage key of this generation.

        Returns: Lineage key of this generation.
        """
        return self._parent

    @property
    def tag(self) -> 'Generation.Tag':
        """Generation metadata. In case of implicit generation and empty lineage this returns a "null" tag (a Tag object
        with all fields empty)

        Returns: Generation tag (metadata) object.
        """
        if not self._tag:
            lineage = self.lineage.key  # lineage must exist so let's fetch it outside of try-except
            try:
                self._tag = self._registry.open(self.project.key, lineage, self.key)
            except self.Listing.Empty:  # generation doesn't exist
                LOGGER.debug('No previous generations found - using a null tag')
                return self.NOTAG
        return self._tag

    def list(self) -> Level.Listing[uuid.UUID]:
        """Return the listing of this level.

        Returns: Level listing.
        """
        return self.Listing(self.tag.states)

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
            raise Level.Invalid(f'Unknown state reference for {self}: {sid}')
        LOGGER.debug('%s: Loading state %s', self, sid)
        return self._registry.read(self.project.key, self.lineage.key, self.key, sid)


class Lineage(Level[version.Version, int]):
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, registry: 'persistent.Registry', project: 'Project',
                 key: typing.Optional[typing.Union[str, version.Version]] = None):
        if key:
            key = version.Version(str(key))
        super().__init__(registry, key, parent=project)
        self._artifact: typing.Optional['product.Artifact'] = None

    @property
    def project(self) -> 'Project':
        """Get the project of this generation.

        Returns: Project of this generation.
        """
        return self._parent

    @property
    def artifact(self) -> 'product.Artifact':
        """Lineage artifact.

        Returns: Artifact object.
        """
        if not self._artifact:
            self._artifact = self._registry.mount(self.project.key, self.key)
        return self._artifact

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an unbound state (not belonging to any project) under given state id.

        An unbound state is expected to be committed later into a new generation of specific lineage.

        Args:
            state: Serialized state to be persisted.

        Returns: Associated state id.
        """
        sid = uuid.uuid4()
        LOGGER.debug('%s: Dumping state %s', self, sid)
        self._registry.write(self.project.key, self.key, sid, state)
        return sid

    def list(self) -> Level.Listing[int]:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self._registry.generations(self.project.key, self.key)

    def get(self, generation: typing.Optional[int] = None) -> Generation:
        """Get a generation instance by its id.

        Args:
            generation: Integer generation id.

        Returns: Generation instance.
        """
        return Generation(self._registry, self, generation)

    def put(self, tag: Generation.Tag) -> Generation:
        """Commit a new generation described by its tag. All states listed on the tag are expected to have
        been provided previously by individual dumps.

        Args:
            tag: Generation metadata.

        Returns: Generation instance.
        """
        try:
            generation = self.list().last + 1
        except self.Listing.Empty:
            generation = 1
        self._registry.close(self.project.key, self.key, generation, tag)
        return self.get(generation)


class Project(Level[str, version.Version]):
    """Sequence of lineages based on same project.
    """
    def __init__(self, registry: 'persistent.Registry', root: 'Root', key: str):
        super().__init__(registry, str(key), parent=root)

    def list(self) -> Level.Listing[version.Version]:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self._registry.lineages(self.key)

    def get(self, lineage: typing.Optional[version.Version] = None) -> Lineage:
        """Get a lineage instance by its id.

        Args:
            lineage: Lineage version.

        Returns: Lineage instance.
        """
        return Lineage(self._registry, self, lineage)

    def put(self, package: 'distribution.Package') -> Lineage:
        """Publish new lineage to the repository based on provided package.

        Args:
            package: Distribution package to be persisted.

        Returns: new lineage instance based on the package.
        """
        project = package.manifest.name
        lineage = package.manifest.version
        try:
            previous = self.list().last
        except (Level.Invalid, Level.Listing.Empty):
            LOGGER.debug('No previous lineage for %s-%s', project, lineage)
        else:
            if project != self.key:
                raise error.Invalid(f'Project key mismatch')
            if not lineage > previous:
                raise Level.Invalid(f'{project}-{lineage} not an increment from existing {previous}')
        self._registry.push(package)
        return self.get(lineage)


class Root(Level[None, str]):
    """Sequence of projects.
    """
    def __init__(self, registry: 'persistent.Registry'):  # pylint: disable=useless-super-delegation
        super().__init__(registry)

    def list(self) -> Level.Listing[str]:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self._registry.projects()

    def get(self, project: str) -> Project:
        """Get a project instance by its name.

        Args:
            project: Project name.

        Returns: Project instance.
        """
        return Project(self._registry, self, project)

    @property
    def key(self) -> None:
        """No key for the root.
        """
        return None
