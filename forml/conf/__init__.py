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
import os
import pathlib
import re
import sys
import tempfile
import types
import typing

import toml

import forml
from forml.provider import feed, gateway, inventory, registry, runner, sink


class Parser(dict):
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
            self.update(toml.load(path))
        except FileNotFoundError:  # not an error (ignore)
            pass
        except PermissionError as err:  # soft error (warn)
            self._errors[path] = err
        except ValueError as err:  # hard error (abort)
            raise forml.InvalidError(f'Invalid config file {path}: {err}') from err
        else:
            self._sources.append(path)


SECTION_PLATFORM = 'PLATFORM'
SECTION_REGISTRY = 'REGISTRY'
SECTION_FEED = 'FEED'
SECTION_SINK = 'SINK'
SECTION_RUNNER = 'RUNNER'
SECTION_INVENTORY = 'INVENTORY'
SECTION_GATEWAY = 'GATEWAY'
SECTION_TESTING = 'TESTING'
OPT_LOGCFG = 'logcfg'
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

DEFAULTS = {
    # all static defaults should go rather to the ./config.toml (in this package)
    OPT_TMPDIR: tempfile.gettempdir(),
    SECTION_REGISTRY: {OPT_PATH: [registry.__name__]},
    SECTION_RUNNER: {OPT_PATH: [runner.__name__]},
    SECTION_FEED: {OPT_PATH: [feed.__name__]},
    SECTION_SINK: {OPT_PATH: [sink.__name__]},
    SECTION_INVENTORY: {OPT_PATH: [inventory.__name__]},
    SECTION_GATEWAY: {OPT_PATH: [gateway.__name__]},
}

APPNAME = 'forml'
SYSDIR = pathlib.Path('/etc') / APPNAME
USRDIR = pathlib.Path(os.getenv(f'{APPNAME.upper()}_HOME', pathlib.Path.home() / f'.{APPNAME}'))
PATH = pathlib.Path(__file__).parent, SYSDIR, USRDIR
APPCFG = 'config.toml'
PRJNAME = re.sub(r'\.[^.]*$', '', pathlib.Path(sys.argv[0]).name)

PARSER = Parser(DEFAULTS, *(p / APPCFG for p in PATH))

for _path in (USRDIR, SYSDIR):
    if _path not in sys.path:
        sys.path.append(str(_path))


def __getattr__(key: str):
    try:
        return PARSER[key]
    except KeyError as err:
        raise AttributeError(err) from err
