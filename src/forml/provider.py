# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Provider management.
"""
import abc
import collections
import inspect
import logging
import typing

from forml import error

LOGGER = logging.getLogger(__name__)


class Registry(collections.namedtuple('Registry', 'provider, paths')):
    """Registry of providers of certain interface. It is a tuple of (not-yet-imported) search paths and already
    imported providers.
    """
    class Path(typing.NamedTuple):
        """Search paths for loading. If flagged as explicit, loading such a path would fail if not found.
        """
        value: str
        explicit: bool = False  # whether to fail on import errors

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            return other.__class__ is self.__class__ and other.value == self.value

        def __truediv__(self, suffix: str) -> 'Registry.Path':
            return Registry.Path(f'{self.value}.{suffix}', explicit=False)

        def load(self) -> None:
            """Load the package modules.
            """
            LOGGER.debug('Attempting to import %s', self.value)
            try:
                __import__(self.value, fromlist=['*'])
            except ModuleNotFoundError as err:
                if self.explicit:
                    raise error.Failed(f'Explicit preload ({self.value}) failed ({err})') from err
                return

    def __new__(cls):
        return super().__new__(cls, dict(), set())

    def add(self, provider: typing.Type['Interface'], alias: typing.Optional[str], paths: typing.Set[Path]):
        """Push package to lazy loading stack.

        Args:
            provider: implementation class
            alias: provider alias
            paths: search paths to be explored when attempting to load
        """
        references = {repr(provider)}
        if alias:
            references.add(alias)
        for key in references:
            if key in self.provider:
                if provider == self.provider[key]:
                    continue
                raise error.Unexpected(f'Provider key collision ({key})')
        self.paths.update(paths)
        if inspect.isabstract(provider):
            return
        LOGGER.debug('Registering provider %s as `%s` with %d more search paths %s',
                     provider.__name__, key, len(paths), paths)
        for key in references:
            self.provider[key] = provider

    def get(self, key: str) -> typing.Type['Interface']:
        """Get the registered provider or attempt to load all search paths packages that might be containing it.

        Args:
            key: provider key

        Returns: Registered provider.
        """
        LOGGER.debug('Getting provider of %s (%d search paths)', key, len(self.paths))
        if key not in self.provider:
            pending = list(self.paths)
            for path in self.paths:
                pending.append(path / key)
            path = key
            while '.' in path:
                path, _ = path.rsplit('.', 1)
                pending.append(Registry.Path(path))
            while key not in self.provider and pending:
                pending.pop().load()
        return self.provider[key]


REGISTRY: typing.Dict[typing.Type['Interface'], Registry] = collections.defaultdict(Registry)
DEFAULTS: typing.Dict[typing.Type['Interface'], typing.Tuple[str, typing.Mapping[str, typing.Any]]] = dict()


class Meta(abc.ABCMeta):
    """Provider metaclass.
    """
    def __new__(mcs, name, bases, namespace,
                default: typing.Optional[typing.Tuple[str, typing.Mapping[str, typing.Any]]] = None, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if default:
            if not inspect.isabstract(cls):
                raise error.Unexpected('Defaults provided but class not abstract')
            DEFAULTS[cls] = default
        return cls

    def __call__(cls, *args, **kwargs) -> 'Interface':
        if cls in DEFAULTS:
            key, params = DEFAULTS[cls]
            return cls[key](*args, **{**params, **kwargs})  # pylint: disable=unsubscriptable-object
        return super().__call__(*args, **kwargs)

    def __getitem__(cls, key: typing.Any) -> typing.Type['Interface']:
        if not isinstance(key, str) and issubclass(cls, typing.Generic):
            return cls.__class_getitem__(key)
        try:
            return REGISTRY[cls].get(key)
        except KeyError as err:
            raise error.Missing(
                f'No {cls.__name__} provider registered as {key} (known providers: {", ".join(cls)})') from err

    def __iter__(cls):
        return iter(REGISTRY[cls].provider)

    def __repr__(cls):
        return f'{cls.__module__}.{cls.__qualname__}'

    def __str__(cls):
        return f'{repr(cls)}[{", ".join(cls)}]'

    def __eq__(cls, other):
        return other.__module__ is cls.__module__ and other.__qualname__ is cls.__qualname__

    def __hash__(cls):
        return hash(cls.__module__) ^ hash(cls.__qualname__)


class Interface(metaclass=Meta):
    """Base class for service providers.
    """
    def __init_subclass__(cls, alias: typing.Optional[str] = None, path: typing.Optional[typing.Iterable[str]] = None):
        """Register the provider based on its optional key.

        Normally would be implemented using the Meta.__init__ but it needs the Interface class to exist.

        Args:
            alias: Optional key to register the provider as (in addition to its qualified name).
            path: Optional search path for additional packages to get imported when attempting to load.
        """
        super().__init_subclass__()
        if inspect.isabstract(cls) and alias:
            raise error.Unexpected(f'Provider key ({alias}) illegal on abstract class')
        path = {Registry.Path(p, explicit=True) for p in path or []}
        for parent in (p for p in cls.__mro__ if issubclass(p, Interface) and p is not Interface):
            REGISTRY[parent].add(cls, alias, path)
