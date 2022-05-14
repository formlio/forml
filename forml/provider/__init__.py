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

import forml

LOGGER = logging.getLogger(__name__)


def isabstract(cls: type['Service']) -> bool:
    """Extended version of inspect.isabstract that also considers any inner classes.

    Args:
        cls: Class to be inspected.

    Returns:
        True if class is abstract or has an abstract inner class.
    """
    return inspect.isabstract(cls) or any(inspect.isabstract(i) for i in cls.__dict__.values())


class Reference:
    """Provider reference base class/dispatcher."""

    def __new__(cls, value: typing.Union[type['Service'], str]):
        if isinstance(value, str):
            if Qualifier.DELIMITER not in value:
                return Alias(value)
            module, qualname = value.split(Qualifier.DELIMITER, 1)
        else:
            module = value.__module__
            qualname = value.__qualname__
        return Qualifier(module, qualname)

    @abc.abstractmethod
    def paths(self, base: typing.Optional[typing.Iterable['Bank.Path']] = None) -> typing.Iterable['Bank.Path']:
        """Return potential search paths for importing this referenced provider.

        Args:
            base: Optional base paths of the reference.

        Returns:
            Registry path instances.
        """


class Qualifier(collections.namedtuple('Qualifier', 'module, qualname'), Reference):
    """Reference determining the provider class within its module."""

    module: str
    qualname: str
    DELIMITER = ':'

    def __repr__(self):
        return f'{self.module}{self.DELIMITER}{self.qualname}'

    def paths(self, base: typing.Optional[typing.Iterable['Bank.Path']] = None) -> typing.Iterable['Bank.Path']:
        return tuple([Bank.Path(self.module, explicit=False)])


class Alias(str, Reference):
    """Reference as a plain string associated with the provider by its author."""

    def __new__(cls, value: str):
        if Qualifier.DELIMITER in value:
            raise ValueError(f'Invalid alias: {value}')
        return super().__new__(cls, value)

    def paths(self, base: typing.Optional[typing.Iterable['Bank.Path']] = None) -> typing.Iterable['Bank.Path']:
        return tuple(b / self for b in base or [])


class Bank(collections.namedtuple('Registry', 'provider, paths')):
    """Registry of providers of certain interface. It is a tuple of (not-yet-imported) search paths and already
    imported providers.
    """

    class Path(typing.NamedTuple):
        """Search paths for loading. If flagged as explicit, loading such a path would fail if not found."""

        value: str
        explicit: bool = False  # whether to fail on import errors

        def __hash__(self):
            return hash(self.value)

        def __eq__(self, other):
            return other.__class__ is self.__class__ and other.value == self.value

        def __truediv__(self, suffix: str) -> 'Bank.Path':
            return Bank.Path(f'{self.value}.{suffix}', explicit=False)

        def load(self) -> None:
            """Load the package modules."""
            LOGGER.debug('Attempting to import %s', self.value)
            try:
                __import__(self.value, fromlist=['*'])
            except ModuleNotFoundError as err:
                if not self.value.startswith(err.name):
                    raise err
                if self.explicit:
                    raise forml.MissingError(f'Explicit preload {self.value} not found') from err
                return

    def __new__(cls):
        return super().__new__(cls, dict(), set())  # pylint: disable=use-dict-literal

    def add(self, provider: type['Service'], alias: typing.Optional[Alias], paths: set[Path]):
        """Push package to lazy loading stack.

        Args:
            provider: implementation class
            alias: provider alias
            paths: search paths to be explored when attempting to load
        """
        references = {Reference(provider)}
        if alias:
            references.add(alias)
        for ref in references:
            if ref in self.provider:
                if provider == self.provider[ref]:
                    continue
                raise forml.UnexpectedError(f'Provider reference collision ({ref})')
        self.paths.update(paths)
        if isabstract(provider):
            return
        for ref in references:
            LOGGER.debug(
                'Registering provider %s as `%s` with %d more search paths %s',
                provider.__name__,
                ref,
                len(paths),
                paths,
            )
            self.provider[ref] = provider

    def get(self, reference: Reference) -> type['Service']:
        """Get the registered provider or attempt to load all search paths packages that might be containing it.

        Args:
            reference: provider reference

        Returns:
            Registered provider.
        """
        LOGGER.debug('Getting provider of %s (%d search paths)', reference, len(self.paths))
        if reference not in self.provider:
            paths = [*self.paths, *reference.paths(self.paths)]
            while reference not in self.provider and paths:
                paths.pop().load()
        return self.provider[reference]


BANK: dict[type['Service'], Bank] = collections.defaultdict(Bank)
DEFAULTS: dict[type['Service'], tuple[str, typing.Mapping[str, typing.Any]]] = {}


class Meta(abc.ABCMeta):
    """Provider metaclass."""

    def __new__(
        mcs,
        name,
        bases,
        namespace,
        default: typing.Optional[tuple[str, typing.Mapping[str, typing.Any]]] = None,
        **kwargs,
    ):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if default:
            if not isabstract(cls):
                raise forml.UnexpectedError('Defaults provided but class not abstract')
            DEFAULTS[cls] = default
        return cls

    def __call__(cls, *args, **kwargs) -> 'Service':
        if cls in DEFAULTS:
            reference, params = DEFAULTS[cls]
            return cls[reference](*args, **params | kwargs)  # pylint: disable=unsubscriptable-object
        return super().__call__(*args, **kwargs)

    def __getitem__(cls, reference: typing.Any) -> type['Service']:
        if not isinstance(reference, str) and issubclass(cls, typing.Generic):
            return cls.__class_getitem__(reference)
        try:
            return BANK[cls].get(Reference(reference))
        except KeyError as err:
            known = ', '.join(str(c) for c in cls)  # pylint: disable=not-an-iterable
            raise forml.MissingError(
                f'No {cls.__name__} provider registered as {reference} (known providers: {known})'
            ) from err

    def __iter__(cls):
        return iter(BANK[cls].provider)

    def __repr__(cls):
        return repr(Reference(cls))

    def __str__(cls):
        return f'{repr(cls)}[{", ".join(str(c) for c in cls)}]'  # pylint: disable=not-an-iterable

    def __eq__(cls, other):
        return isinstance(other, Meta) and other.__module__ == cls.__module__ and other.__qualname__ is cls.__qualname__

    def __hash__(cls):
        return hash(cls.__module__) ^ hash(cls.__qualname__)


class Service(metaclass=Meta):
    """Base class for service providers."""

    def __init_subclass__(cls, alias: typing.Optional[str] = None, path: typing.Optional[typing.Iterable[str]] = None):
        """Register the provider based on its optional reference.

        Normally would be implemented using the Meta.__init__ but it needs the Service class to exist.

        Args:
            alias: Optional reference to register the provider as (in addition to its qualified name).
            path: Optional search path for additional packages to get imported when attempting to load.
        """
        super().__init_subclass__()
        if alias:
            if isabstract(cls):
                raise forml.UnexpectedError(f'Provider reference ({alias}) illegal on abstract class')
            alias = Alias(alias)
        path = {Bank.Path(p, explicit=True) for p in path or []}
        for parent in (p for p in cls.__mro__ if issubclass(p, Service) and p is not Service):
            BANK[parent].add(cls, alias, path)
