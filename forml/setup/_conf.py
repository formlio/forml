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

"""ForML configuration
"""
import abc
import operator
import os
import pathlib
import re
import sys
import tempfile
import types
import typing
from logging import handlers

import tomli

import forml
from forml.provider import feed, gateway, inventory, registry, runner, sink


class Config(dict):
    """Config parser implementation."""

    def __init__(self, defaults: typing.Mapping[str, typing.Any], *paths: pathlib.Path):
        super().__init__()
        self._sources: list[pathlib.Path] = []
        self._errors: dict[pathlib.Path, Exception] = {}
        self._notifiers: list[typing.Callable[[], None]] = []
        self.update(defaults)
        for src in paths:
            self.read(src)

    def subscribe(self, notifier: typing.Callable[[], None]) -> None:
        """Register a callback to be called upon configuration updates.

        Args:
            notifier: Simple callable to be executed when config gets updated.
        """
        self._notifiers.append(notifier)

    def update(self, other: typing.Optional[typing.Mapping[str, typing.Any]] = None, **kwargs) -> None:
        """Parser config gets updated by recursive merge with right (new) scalar values overwriting left (old) ones.

        Args:
            other: Another mapping with config values to be merged with our config.
            **kwargs: Other values provided as keword arguments.
        """

        def merge(left: typing.Mapping, right: typing.Mapping) -> typing.Mapping[str, typing.Any]:
            """Recursive merge of two dictionaries with right-to-left precedence. Any non-scalar, non-dictionary objects
            are copied by reference.

            Args:
                left: Left dictionary to be merged.
                right: Right dictionary to be merged.

            Returns:
                Merged dictionary.
            """
            # pylint: disable=isinstance-second-argument-not-valid-type
            result = {}
            common = set(left).intersection(right)
            for key in set(left).union(right):
                if key in common and isinstance(left[key], typing.Mapping) and isinstance(right[key], typing.Mapping):
                    value = merge(left[key], right[key])
                elif key in common and isinstance(left[key], (list, tuple)) and isinstance(right[key], (list, tuple)):
                    value = *right[key], *(v for v in left[key] if v not in right[key])
                elif key in right:
                    value = right[key]
                else:
                    value = left[key]
                result[key] = value
            return types.MappingProxyType(result)

        super().update(merge(merge(self, other or {}), kwargs))
        for notifier in self._notifiers:
            notifier()

    @property
    def sources(self) -> typing.Iterable[pathlib.Path]:
        """Get the sources files used by this parser.

        Returns:
            Source files.
        """
        return tuple(self._sources)

    @property
    def errors(self) -> typing.Mapping[pathlib.Path, Exception]:
        """Errors captured during parsing.

        Returns:
            Mapping between files and the captured errors.
        """
        return types.MappingProxyType(self._errors)

    def read(self, path: pathlib.Path) -> None:
        """Read and merge config from given file.

        Args:
            path: Path to file to parse.
        """
        try:
            with open(path, 'rb') as cfg:
                self.update(tomli.load(cfg))
        except FileNotFoundError:  # not an error (ignore)
            pass
        except PermissionError as err:  # soft error (warn)
            self._errors[path] = err
        except ValueError as err:  # hard error (abort)
            raise RuntimeError(f'Invalid config file {path}: {err}') from err
        else:
            self._sources.append(path)


class Meta(abc.ABCMeta):
    """Metaclass for parsed config options tht adds the itemgetter properties to the class."""

    def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
        if 'FIELDS' in namespace:
            for index, field in enumerate(namespace.pop('FIELDS')):
                namespace[field] = property(operator.itemgetter(index))
        return super().__new__(mcs, name, (*bases, tuple), namespace)

    @property
    def default(cls) -> 'Section':
        """Default parsing.

        Returns:
            Default resolved config.
        """
        return cls.resolve()


class Section(metaclass=Meta):
    """Resolved config base class.

    Implements parser for config referenced based on following concept:

    [INDEX]
    SELECTOR = reference

    [GROUP.reference]
    <semantic_param> = <value>  # explicit params consumed by the config parser
    <generic_param> = <value>   # generic params not known to the config parser (ie downstream library config)
    params = { <generic_param> = <value> }  # alternative way of providing generic params to avoid collisions
    """

    # list of parsed config field names
    FIELDS: tuple[str] = ('params',)
    # master section containing the references to the particular GROUP sections
    INDEX: str = abc.abstractmethod
    # name of option in INDEX section containing reference(s) to the particular GROUP section
    SELECTOR: str = abc.abstractmethod
    # common name (prefix) of sections referred by SELECTOR
    GROUP: str = abc.abstractmethod

    def __new__(cls, reference: str):
        try:
            kwargs = CONFIG[cls.GROUP][reference]  # pylint: disable=no-member
        except KeyError as err:
            raise forml.MissingError(f'Config section not found: [{cls.GROUP}.{reference}]') from err
        args, kwargs = cls._extract(reference, kwargs)
        return super().__new__(cls, [*args, types.MappingProxyType(dict(kwargs))])

    @classmethod
    def _extract(
        cls, reference: str, kwargs: typing.Mapping[str, typing.Any]  # pylint: disable=unused-argument
    ) -> tuple[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]:
        """Extract the config values as a sequence of "known" semantic arguments and mapping of "generic" options.

        Args:
            reference: Config reference.
            kwargs: Common mapping of values mixing the "known" and "generic".

        Returns:
            Tuple of known plus generic arguments.
        """
        kwargs = dict(kwargs)
        kwargs.update(kwargs.pop(OPT_PARAMS, {}))
        return [], kwargs

    @classmethod
    def _lookup(cls, reference: str) -> 'Section':
        """Create the config instance based on given reference.

        Args:
            reference: Config reference.

        Returns:
            Config instance.
        """
        return cls(reference)

    @classmethod
    def resolve(cls, reference: typing.Optional[str] = None) -> 'Section':
        """Get config list for pattern based non-repeated option tokens.

        Args:
            reference: Config reference.

        Returns:
            Config instance.
        """
        reference = reference or CONFIG.get(cls.INDEX, {}).get(cls.SELECTOR)
        if not reference:
            raise forml.MissingError(f'No default reference [{cls.INDEX}].{cls.SELECTOR}')
        return cls._lookup(reference)

    def __hash__(self):
        return hash(self.__class__) ^ hash(tuple(sorted(self.params)))  # pylint: disable=no-member

    @abc.abstractmethod
    def __lt__(self, other: 'Section') -> bool:
        """Instances need to be comparable to allow for sorting.

        Args:
            other: Right side of the comparison.

        Returns:
            True if left is less than right.
        """


class Multi(Section):  # pylint: disable=abstract-method
    """Resolved section supporting multiple instances.

    [INDEX]
    SELECTOR = [reference1, reference2]

    [GROUP.reference1]
    <params>
    [GROUP.reference2]
    <params>
    """

    @classmethod
    def _lookup(cls, reference: typing.Iterable[str]) -> typing.Sequence[Section]:
        """Create a sequence of config instances based on given references.

        Args:
            reference: Config references.

        Returns:
            Config instances.
        """
        if isinstance(reference, str):
            reference = [reference]
        return tuple(sorted(cls(r) for r in reference))


SECTION_LOGGING = 'LOGGING'
SECTION_TEMPLATING = 'TEMPLATING'
SECTION_PLATFORM = 'PLATFORM'
SECTION_REGISTRY = 'REGISTRY'
SECTION_FEED = 'FEED'
SECTION_SINK = 'SINK'
SECTION_RUNNER = 'RUNNER'
SECTION_INVENTORY = 'INVENTORY'
SECTION_GATEWAY = 'GATEWAY'
SECTION_TESTING = 'TESTING'
OPT_CONFIG = 'config'
OPT_FACILITY = 'facility'
OPT_TMPDIR = 'tmpdir'
OPT_PROVIDER = 'provider'
OPT_PRIORITY = 'priority'
OPT_REGISTRY = 'registry'
OPT_PARAMS = 'params'
OPT_FEED = 'feed'
OPT_SINK = 'sink'
OPT_RUNNER = 'runner'
OPT_DEFAULT = 'default'
OPT_PATH = 'path'
OPT_TRAIN = 'train'
OPT_APPLY = 'apply'
OPT_EVAL = 'eval'

APPNAME = 'forml'
PRJNAME = re.sub(r'\.[^.]*$', '', pathlib.Path(sys.argv[0]).name)
#: System-level setup directory
SYSDIR = pathlib.Path('/etc') / APPNAME
#: User-level setup directory
USRDIR = pathlib.Path(os.getenv(f'{APPNAME.upper()}_HOME', pathlib.Path.home() / f'.{APPNAME}'))
#: Sequence of setup directories in ascending priority order
PATH = pathlib.Path(__file__).parent, SYSDIR, USRDIR
#: Main config file name
APPCFG = 'config.toml'

DEFAULTS = {
    # all static defaults should go rather to the ./config.toml (in this package)
    OPT_TMPDIR: tempfile.gettempdir(),
    SECTION_LOGGING: {
        OPT_FACILITY: handlers.SysLogHandler.LOG_USER,
        OPT_PATH: f'./{PRJNAME}.log',
    },
    SECTION_REGISTRY: {OPT_PATH: [registry.__name__]},
    SECTION_RUNNER: {OPT_PATH: [runner.__name__]},
    SECTION_FEED: {OPT_PATH: [feed.__name__]},
    SECTION_SINK: {OPT_PATH: [sink.__name__]},
    SECTION_INVENTORY: {OPT_PATH: [inventory.__name__]},
    SECTION_GATEWAY: {OPT_PATH: [gateway.__name__]},
}

CONFIG = Config(DEFAULTS, *(p / APPCFG for p in PATH))

tmpdir = CONFIG[OPT_TMPDIR]
