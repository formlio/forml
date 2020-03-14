"""
Provider management.
"""
import abc
import collections
import inspect
import logging
import sys
import typing

from forml import error

LOGGER = logging.getLogger(__name__)


class Registry(collections.namedtuple('Registry', 'stage, provider')):
    """Registry of providers of certain interface. It is a tuple of staged (not-yet-loaded) packages and already
    imported modules.
    """
    class Package(typing.NamedTuple):
        """Provider package to be staged for loading. If flagged as strict loading such a package would fail in case of
        the package or its modules are not found.
        """
        path: str
        strict: bool = True

        def __hash__(self):
            return hash(self.path)

        def __eq__(self, other):
            return other.__class__ is self.__class__ and other.path == self.path

        def load(self) -> None:
            """Load the package modules.
            """
            LOGGER.debug('Attempting to load package %s', self.path)
            try:
                __import__(self.path, fromlist=['*'])
            except ModuleNotFoundError as err:
                if self.strict:
                    raise error.Failed(f'Explicit preload ({self.path}) failed ({err})')
            if not getattr(sys.modules[self.path], '__all__', []):
                LOGGER.warning('Package %s does not list any provider modules under __all__', self.path)

    def __new__(cls):
        return super().__new__(cls, set(), dict())

    def add(self, key: str, provider: typing.Type['Interface'], packages: typing.Set[Package]):
        """Push package to lazy loading stack.

        Args:
            key: provider key
            provider: implementation class
            packages: package paths to put on stage
        """
        if key in self.provider:
            if provider == self.provider[key]:
                return
            raise error.Unexpected(f'Provider key collision ({key})')
        self.stage.update(packages)
        if inspect.isabstract(provider):
            return
        LOGGER.debug('Registering provider %s as "%s" with %d more packages staged %s',
                     provider.__name__, key, len(packages), packages)
        self.provider[key] = provider

    def get(self, key: str) -> typing.Type['Interface']:
        """Get the registered provider or attempt to load all pre-staged packages that might be containing it.

        Args:
            key: provider key

        Returns: Registered provider.
        """
        LOGGER.debug('Getting provider of %s (%d staged)', key, len(self.stage))
        while key not in self.provider and self.stage:
            self.stage.pop().load()
        return self.provider[key]


_REGISTRY: typing.Dict[typing.Type['Interface'], Registry] = collections.defaultdict(Registry)


class Meta(abc.ABCMeta):
    """Provider metaclass.
    """
    DEFAULTS: typing.Dict[typing.Type['Interface'], typing.Tuple[str, typing.Mapping[str, typing.Any]]] = dict()

    def __new__(mcs, name, bases, namespace,
                default: typing.Optional[typing.Tuple[str, typing.Mapping[str, typing.Any]]] = None, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if default:
            if not inspect.isabstract(cls):
                raise error.Unexpected('Defaults provided but class not abstract')
            mcs.DEFAULTS[cls] = default
        return cls

    def __call__(cls, *args, **kwargs):
        if cls in Meta.DEFAULTS:
            key, params = Meta.DEFAULTS[cls]
            return cls[key](*args, **{**params, **kwargs})  # pylint: disable=unsubscriptable-object
        return super().__call__(*args, **kwargs)

    def __getitem__(cls, key):
        try:
            return _REGISTRY[cls].get(key)
        except KeyError:
            raise error.Missing(f'No {cls.__name__} provider registered as {key} (known providers: {", ".join(cls)})')

    def __iter__(cls):
        return iter(_REGISTRY[cls].provider)

    def __str__(cls):
        return f'{cls.__module__}.{cls.__name__}[{", ".join(cls)}]'

    def __eq__(cls, other):
        return other.__module__ is cls.__module__ and other.__qualname__ is cls.__qualname__

    def __hash__(cls):
        return hash(cls.__module__) ^ hash(cls.__qualname__)


class Interface(metaclass=Meta):
    """Base class for service providers.
    """
    def __init_subclass__(cls, key: typing.Optional[str] = None,
                          packages: typing.Optional[typing.Iterable[str]] = None):
        if inspect.isabstract(cls) and key:
            raise error.Unexpected(f'Provider key ({key}) illegal on abstract class')
        if not key:
            key = cls.__name__
        packages = {Registry.Package(p if '.' not in p else f'{cls.__module__}.{p}') for p in packages or []}
        if not packages:
            packages = {Registry.Package(f'{cls.__module__}.{cls.__name__.lower()}', strict=False)}
        for parent in (p for p in cls.__mro__ if issubclass(p, Interface) and p is not Interface):
            _REGISTRY[parent].add(key, cls, packages)
