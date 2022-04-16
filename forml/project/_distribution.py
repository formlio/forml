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
Project distribution.
"""
import collections
import functools
import json
import logging
import pathlib
import re
import shutil
import string
import sys
import tempfile
import types
import typing
import zipfile

import forml
from forml.io import asset

from . import _body, _importer

LOGGER = logging.getLogger(__name__)


class Package(collections.namedtuple('Package', 'path, manifest')):
    """Distribution package."""

    path: pathlib.Path
    manifest: 'Manifest'

    FORMAT = '4ml'
    COMPRESSION = zipfile.ZIP_DEFLATED
    PYSFX = re.compile(r'\.py[co]?$')

    def __new__(cls, path: typing.Union[str, pathlib.Path]):
        path = pathlib.Path(path)
        return super().__new__(cls, path.resolve(), Manifest.read(path))

    def __getnewargs__(self):
        return tuple([self.path])

    @classmethod
    def create(
        cls, source: typing.Union[str, pathlib.Path], manifest: 'Manifest', path: typing.Union[str, pathlib.Path]
    ) -> 'Package':
        """Create new package from given source tree.

        Args:
            source: Filesystem path to the root of directory tree to be packaged.
            manifest: Package manifest to be used.
            path: Target package filesystem path.

        Returns:
            Package instance.
        """

        def writeall(level: pathlib.Path, archive: zipfile.ZipFile, root: typing.Optional[pathlib.Path] = None) -> None:
            """Recursive helper for adding directory tree content to an zip archive.

            Args:
                level: Level to be added.
                archive: zipfile instance opened for writing.
                root: Root of directory tree to be added.
            """

            def valid(file: pathlib.Path) -> bool:
                """Check the item is valid package item candidate.

                Args:
                    file: Item path to be validated.

                Returns:
                    True if valid.
                """
                return file != descriptor

            if not root:
                root = level
            for item in level.iterdir():
                if not item.is_dir():
                    target = item.relative_to(root)
                    if valid(target):
                        archive.write(item, target)
                else:
                    writeall(item, archive, root)

        descriptor = Manifest.path('.')
        with zipfile.PyZipFile(path, 'w', cls.COMPRESSION) as package:
            with tempfile.TemporaryDirectory() as temp:
                manifest.write(temp)
                package.write(Manifest.path(temp), descriptor)
            writeall(pathlib.Path(source), package)
        return cls(path)

    def install(self, path: typing.Union[str, pathlib.Path]) -> '_body.Artifact':
        """Return the project artifact based on this package mounted on given path.

        Args:
            path: Target install path.

        Returns:
            Artifact instance.
        """

        def uninstalled() -> bool:
            """Prune the existing path if not matching the target manifest.

            Returns:
                True if uninstalled.
            """
            try:
                # to allow installing "virtual" packages this even ignores invalid self.path
                if Manifest.read(path) == self.manifest:
                    LOGGER.debug('Package %s already installed', self.path)
                    return False
            except forml.InvalidError:
                pass
            if path.exists():
                LOGGER.warning('Deleting existing content at %s', path)
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            return True

        path = pathlib.Path(path)
        if path.exists() and path.samefile(self.path):
            LOGGER.debug('Same source-target install attempt ignored')
        elif uninstalled():
            if not zipfile.is_zipfile(self.path):
                assert self.path.is_dir(), f'Expecting zip file or directory: {self.path}'
                LOGGER.debug('Installing directory based package %s to %s', self.path, path)
                shutil.copytree(self.path, path, ignore=lambda *_: {'__pycache__'})
            else:
                with zipfile.ZipFile(self.path) as package:
                    if all(self.PYSFX.search(n) for n in package.namelist()):  # is a zip-safe
                        LOGGER.debug('Installing zip-safe package %s to %s', self.path, path)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(self.path.read_bytes())
                    else:
                        LOGGER.debug('Extracting non zip-safe package %s to %s', self.path, path)
                        package.extractall(path)
        _importer.search(path)
        return _body.Artifact(path, self.manifest.package, **self.manifest.modules)


class Manifest(collections.namedtuple('Manifest', 'name, version, package, modules')):
    """Distribution manifest implementation."""

    name: asset.Project.Key
    version: asset.Release.Key
    package: str
    modules: typing.Mapping[str, str]

    MODULE = f'__{Package.FORMAT}__'
    TEMPLATE = string.Template(
        '\n'.join(
            (
                'NAME = "$name"',
                'VERSION = "$version"',
                'PACKAGE = "$package"',
                'MODULES = $modules',
            )
        )
    )

    def __new__(
        cls,
        name: typing.Union[str, asset.Project.Key],
        version: typing.Union[str, asset.Release.Key],
        package: str,
        **modules: str,
    ):
        return super().__new__(
            cls, asset.Project.Key(name), asset.Release.Key(version), package, types.MappingProxyType(modules)
        )

    def __getnewargs_ex__(self):
        return (self.name, self.version, self.package), dict(self.modules)

    def __repr__(self):
        return f'{self.name}-{self.version}'

    @classmethod
    @functools.cache
    def path(cls, base: typing.Union[str, pathlib.Path]) -> pathlib.Path:
        """Return the manifest module path.

        Args:
            base: Base path for the module.

        Returns:
            Module path.
        """
        return pathlib.Path(base) / f'{cls.MODULE}.py'

    def write(self, path: typing.Union[str, pathlib.Path]) -> None:
        """Write the manifest to given path (directory).

        Args:
            path: Directory to write the manifest into.
        """
        path = self.path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as manifest:
            manifest.write(
                self.TEMPLATE.substitute(
                    name=self.name, version=self.version, package=self.package, modules=json.dumps(dict(self.modules))
                )
            )

    @classmethod
    def read(cls, path: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> 'Manifest':
        """Import the manifest content.

        Args:
            path: Path to import from.

        Returns:
            Manifest instance.
        """
        try:
            module = _importer.isolated(cls.MODULE, path)
            manifest = cls(module.NAME, module.VERSION, module.PACKAGE, **module.MODULES)
        except ModuleNotFoundError as err:
            raise forml.MissingError(f'Unknown manifest ({err})')
        except AttributeError as err:
            raise forml.InvalidError(f'Invalid manifest ({err})')
        finally:
            if cls.MODULE in sys.modules:
                del sys.modules[cls.MODULE]
        return manifest
