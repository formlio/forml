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


class Parser(dict):
    """Config parser implementation.
    """
    def __init__(self, defaults: typing.Mapping[str, typing.Any], *paths: pathlib.Path):
        super().__init__(defaults)
        self._sources: typing.List[pathlib.Path] = list()
        self._errors: typing.Dict[pathlib.Path, Exception] = dict()
        self._notifiers: typing.List[typing.Callable[[], None]] = list()
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

            Returns: Merged dictionary.
            """
            # pylint: disable=isinstance-second-argument-not-valid-type
            common = set(left).intersection(right)
            return {k: merge(left[k], right[k]) if k in common and
                    isinstance(left[k], typing.Mapping) and isinstance(right[k], typing.Mapping) else
                    right[k] if k in right else left[k] for k in set(left).union(right)}
        super().update(merge(merge(self, other or {}), kwargs))
        for notifier in self._notifiers:
            notifier()

    @property
    def sources(self) -> typing.Iterable[pathlib.Path]:
        """Get the sources files used by this parser.

        Returns: Source files.
        """
        return tuple(self._sources)

    @property
    def errors(self) -> typing.Mapping[pathlib.Path, Exception]:
        """Errors captured during parsing.

        Returns: Mapping between files and the captured errors.
        """
        return types.MappingProxyType(self._errors)

    def read(self, path: pathlib.Path) -> None:
        """Read and merge config from given file.

        Args:
            path: Path to file to parse.
        """
        try:
            self.update(toml.load(path))
        except IOError as err:
            self._errors[path] = err
        else:
            self._sources.append(path)


SECTION_PLATFORM = 'PLATFORM'
SECTION_REGISTRY = 'REGISTRY'
SECTION_FEED = 'FEED'
SECTION_RUNNER = 'RUNNER'
SECTION_TESTING = 'TESTING'
OPT_LOGCFG = 'logcfg'
OPT_TMPDIR = 'tmpdir'
OPT_PROVIDER = 'provider'
OPT_PRIORITY = 'priority'
OPT_REGISTRY = 'registry'
OPT_FEED = 'feed'
OPT_RUNNER = 'runner'

DEFAULTS = {
    OPT_LOGCFG: 'logging.ini',
    OPT_TMPDIR: tempfile.gettempdir(),
    SECTION_PLATFORM: {
        OPT_FEED: 'blabol',
        OPT_REGISTRY: 'virtual',
        OPT_RUNNER: 'dask',
    }
}

APPNAME = 'forml'
SYSDIR = pathlib.Path('/etc') / APPNAME
USRDIR = pathlib.Path(os.getenv(f'{APPNAME.upper()}_HOME', pathlib.Path.home() / f'.{APPNAME}'))
PATH = pathlib.Path(__file__).parent, SYSDIR, USRDIR
APPCFG = 'config.toml'
PRJNAME = re.sub(r'\.[^.]*$', '', pathlib.Path(sys.argv[0]).name)


PARSER = Parser(DEFAULTS, *(p / APPCFG for p in PATH))


def __getattr__(key: str):
    try:
        return PARSER[key]
    except KeyError as err:
        raise AttributeError(err) from err
