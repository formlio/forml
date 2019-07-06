"""
Project distribution.
"""
import collections
import importlib
import json
import logging
import os
import shutil
import string
import sys
import types
import typing
import zipfile

from forml import project, conf


LOGGER = logging.getLogger(__name__)


class Error(project.Error):
    """Distribution error.
    """


class Package(collections.namedtuple('Package', 'path, manifest, staging')):
    """Distribution package.
    """
    FORMAT = 'mlp'

    def __new__(cls, path: str, manifest: 'Manifest', staging: str):
        return super().__new__(cls, os.path.abspath(path), manifest, os.path.abspath(staging))

    @classmethod
    def load(cls, path: str, staging: str = conf.) -> 'Package':
        """Load the package.

        Args:
            path: Package path.
            staging: Staging directory for extracted package.

        Returns: Package instance.
        """
        return cls(path, Manifest.read(path), staging)

    @property
    def staged(self) -> str:
        path = os.path.join(self.staging, self.manifest.name, self.manifest.version)
        if os.path.exists(path):
            try:
                existing = Manifest.read(path)
            except Error:
                LOGGER.warning('Corrupted staging')
            else:
                if existing == self.manifest:
                    LOGGER.debug('Existing staging')
                    return path
                LOGGER.warning('Colliding staging')
            shutil.rmtree(path)

        LOGGER.info('Staging package to %s', path)
        with zipfile.ZipFile(self.path) as package:
            package.extractall(path)
        return path

    @property
    def artifact(self) -> project.Artifact:
        """Return the project artifact based on this package.

        Returns: Artifact instance.
        """
        return project.Artifact(self.staged, self.manifest.package, **self.manifest.modules)


class Manifest(collections.namedtuple('Manifest', 'name, version, package, modules')):
    """Distribution manifest implementation.
    """
    MODULE = f'__{Package.FORMAT}__'
    TEMPLATE = string.Template(
        'NAME = "$name"\n'
        'VERSION = "$version"\n'
        'PACKAGE = "$package"\n'
        'MODULES = $modules\n')

    class Reader:
        """Context manager for loading manifest content.
        """
        class Importer:
            """Importer context.
            """
            def __init__(self, path: typing.Optional[str] = None):
                self.path: typing.Optional[str] = path

            def _unload(self) -> None:
                """Unload the module.
                """
                if Manifest.MODULE in sys.modules:
                    del sys.modules[Manifest.MODULE]

            def __enter__(self) -> None:
                self._unload()
                if self.path:
                    sys.path.insert(0, self.path)

            def __exit__(self, exc_type, exc_val, exc_tb):
                if sys.path and self.path == sys.path[0]:
                    sys.path.pop(0)
                self._unload()
                if exc_type is ModuleNotFoundError:
                    raise Error(f'Unknown manifest ({exc_val})')

        def __init__(self, path: typing.Optional[str] = None):
            self._importer: Manifest.Reader.Importer = self.Importer(path)

        def __enter__(self) -> types.ModuleType:
            with self._importer:
                return importlib.import_module(Manifest.MODULE)

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is AttributeError:
                raise Error(f'Invalid manifest ({exc_val})')

    def __new__(cls, name: str, version: str, package: str, **modules: str):
        return super().__new__(cls, name, version, package, types.MappingProxyType(modules))

    def __getnewargs_ex__(self):
        return (self.name, self.version, self.package), dict(self.modules)

    def write(self, path: str) -> None:
        """Write the manifest to given path (directory).

        Args:
            path: Directory to write the manifest into.
        """
        with open(os.path.join(path, f'{self.MODULE}.py'), 'w') as manifest:
            manifest.write(self.TEMPLATE.substitute(name=self.name, version=self.version, package=self.package,
                                                    modules=json.dumps(dict(self.modules))))

    @classmethod
    def read(cls, path: typing.Optional[str] = None) -> 'Manifest':
        """Import the manifest content.

        Args:
            path: Path to import from

        Returns: Manifest instance.
        """
        with cls.Reader(path) as manifest:
            return cls(manifest.NAME, manifest.VERSION, manifest.PACKAGE, **manifest.MODULES)
