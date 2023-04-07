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
Customized setuptools.
"""
import collections
import functools
import inspect
import logging
import os
import pathlib
import sys
import types
import typing

import setuptools
import tomli
from setuptools import dist

import forml
from forml import setup

from ...io import asset
from .. import _body
from .command import bdist, launch, upload

LOGGER = logging.getLogger(__name__)


class Distribution(dist.Distribution):  # pylint: disable=function-redefined
    """Extended distribution type with extra ForML attributes."""

    COMMANDS: typing.Mapping[str, type[setuptools.Command]] = {
        'train': launch.Train,
        'tune': launch.Tune,
        'eval': launch.Eval,
        bdist.Package.COMMAND: bdist.Package,
        'upload': upload.Registry,
    }

    def __init__(self, attrs: typing.Optional[typing.Mapping[str, typing.Any]] = None):
        attrs = dict(attrs or ())
        self.tree: Tree = attrs.pop('tree', None) or Tree(attrs.get('package_dir', {}).get('', '.'))
        attrs.setdefault('cmdclass', {}).update(self.COMMANDS)
        attrs['name'] = str(self.tree.name)
        attrs['version'] = str(self.tree.version)
        # attrs['install_requires'] = self.tree.dependencies
        attrs['packages'] = setuptools.find_packages(include=[f'{self.tree.package}*'], where=self.tree.path)
        super().__init__(attrs)

    def run_commands(self):
        """Overriding the default functionality to allow bypassing the execution if not called
        directly.

        This is to avoid fork looping e.g. when using Dask multiprocessing runner.
        """
        frame = inspect.currentframe()
        while frame:
            name = frame.f_globals.get('__name__')
            if name == '__mp_main__':
                return
            frame = frame.f_back
        super().run_commands()

    @functools.cached_property
    def artifact(self) -> _body.Artifact:
        """Get the project artifact.

        Returns:
            Artifact instance.
        """
        return _body.Artifact(self.tree.path, self.tree.package, **self.tree.components)


class Tree(collections.namedtuple('Tree', 'path')):
    """Project source tree handle."""

    path: pathlib.Path
    """The project tree root directory."""

    _PPCFG_FILE = 'pyproject.toml'
    _PPKEY_TOOL = 'tool'
    _PPKEY_FORML = 'forml'
    _PPKEY_PACKAGE = 'package'
    _PPKEY_PROJECT = 'project'
    _PPKEY_VERSION = 'version'
    _PPKEY_NAME = 'name'
    _PPKEY_COMPONENTS = 'components'
    _PPKEY_DEPENDENCIES = 'dependencies'
    DEFAULT_VERSION = '0.1.dev1'
    DEFAULT_NAME = setup.PRJNAME

    def __new__(cls, path: typing.Union[str, pathlib.Path]):
        return super().__new__(cls, pathlib.Path(path).absolute())

    @functools.cached_property
    def _config(self) -> typing.Mapping[str, typing.Any]:
        try:
            with (self.path / self._PPCFG_FILE).open('rb') as pyproject:
                return tomli.load(pyproject)
        except OSError as err:
            raise forml.MissingError(f'Invalid ForML project: {err}') from err
        except tomli.TOMLDecodeError as err:
            raise forml.InvalidError(f'Invalid ForML project: {err}') from err

    @functools.cached_property
    def name(self) -> asset.Project.Key:
        """Project name getter.

        Returns:
            Project name as defined in the ``project`` table.
        """
        return asset.Project.Key(self._config.get(self._PPKEY_PROJECT, {}).get(self._PPKEY_NAME, self.DEFAULT_NAME))

    @functools.cached_property
    def version(self) -> asset.Release.Key:
        """Project version getter.

        Returns:
            Project version as defined in the ``project`` table.
        """
        return asset.Release.Key(
            self._config.get(self._PPKEY_PROJECT, {}).get(self._PPKEY_VERSION, self.DEFAULT_VERSION)
        )

    @functools.cached_property
    def dependencies(self) -> typing.Sequence[str]:
        """Project dependencies getter.

        Returns:
            Project dependencies as defined in the ``project`` table.
        """
        return tuple(self._config.get(self._PPKEY_PROJECT, {}).get(self._PPKEY_DEPENDENCIES, []))

    @functools.cached_property
    def package(self) -> str:
        """Project package getter.

        Returns:
            Project package as defined in the ``tool.forml`` table.
        """
        tool = self._config.get(self._PPKEY_TOOL, {}).get(self._PPKEY_FORML, {})
        try:
            return tool.get(self._PPKEY_PACKAGE, self.name)
        except KeyError as err:
            raise forml.InvalidError(f'Invalid ForML project: {err}') from err

    @functools.cached_property
    def components(self) -> typing.Mapping[str, typing.Any]:
        """Project components getter.

        Returns:
            Project components as defined in the ``tool.forml`` table.
        """
        tool = self._config.get(self._PPKEY_TOOL, {}).get(self._PPKEY_FORML, {})
        return types.MappingProxyType(dict(tool.get(self._PPKEY_COMPONENTS, {})))

    def run(self, *argv: str, **options) -> None:
        """Run the project mode actions.

        Args:
            argv: Positional arguments to be passed to setuptools.
            options: Keyword options to be passed to setuptools.
        """
        args = [*argv, *(a for k, v in options.items() if v for a in (f'--{k}', v))]
        os.chdir(self.path)
        LOGGER.debug('Launching setuptools under %s using %s', self.path, sys.argv)
        setuptools.setup(script_name=str(self.path / self.name), script_args=args, tree=self, distclass=Distribution)
