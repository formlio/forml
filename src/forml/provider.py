"""
Provider management.
"""
import abc
import collections
import inspect
import logging
import typing

import forml

LOGGER = logging.getLogger(__name__)


class Error(forml.Error):
    """Provider error.
    """


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
                    raise Error(f'Explicit preload ({self.path}) failed ({err})')

    def __new__(cls):
        return super().__new__(cls, set(), dict())

    def add(self, key: str, provider: typing.Type['Interface'], packages: typing.Set[Package]):
        """Push package to lazy loading stack.

        Args:
            packages: package paths to put on stage
        """
        LOGGER.debug('Registering provider %s with %d more packages staged %s', key, len(packages), packages)
        self.stage.update(packages)
        if inspect.isabstract(provider):
            return
        if key in self.provider and self.provider[key] != provider:
            raise Error(f'Provider key collision ({key})')
        self.provider[key] = provider

    def get(self, key: str) -> typing.Type['Interface']:
        """Get the registered provider or attempt to load all pre-staged packages that might be containing it.

        Args:
            key: provider key.

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
    def __getitem__(cls, key):
        try:
            return _REGISTRY[cls].get(key)
        except KeyError:
            raise Error(f'No provider of type {cls.__name__} registered as {key}')

    def __str__(cls):
        return f'{cls.__module__}.{cls.__name__}[{", ".join(_REGISTRY[cls].provider)}]'

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
            raise Error(f'Provider key ({key}) illegal on abstract class')
        if not key:
            key = cls.__name__
        packages = {Registry.Package(p if '.' not in p else f'{cls.__module__}.{p}') for p in packages or []}
        if not packages:
            packages = {Registry.Package(f'{cls.__module__}.{cls.__name__.lower()}', strict=False)}
        for parent in (p for p in cls.__mro__ if issubclass(p, Interface) and p is not Interface):
            _REGISTRY[parent].add(key, cls, packages)
