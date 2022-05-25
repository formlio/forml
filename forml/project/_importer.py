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
Project importer machinery.
"""
import contextlib
import importlib
import itertools
import logging
import pathlib
import re
import sys
import types
import typing
from importlib import abc, machinery

LOGGER = logging.getLogger(__name__)


class Finder(abc.MetaPathFinder):
    """Module path finder implementation."""

    class Loader(abc.Loader):  # pylint: disable=abstract-method
        """Module loader implementation."""

        def __init__(
            self, module: types.ModuleType, onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None
        ):
            self._module: types.ModuleType = module
            self._onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = onexec

        def create_module(self, spec: machinery.ModuleSpec) -> types.ModuleType:
            """Return our fake module instance.

            Args:
                spec: Module spec.

            Returns:
                Module instance.
            """
            return self._module

        def exec_module(self, module: types.ModuleType) -> None:
            """Here we cal the optional onexec handler.

            Args:
                module: to be loaded
            """
            if self._onexec:
                self._onexec(module)

    def __init__(
        self, module: types.ModuleType, onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None
    ):
        self._name: str = module.__name__
        self._loader: abc.Loader = self.Loader(module, onexec)

    def find_spec(
        self,
        fullname: str,
        path: typing.Optional[typing.Sequence[str]],  # pylint: disable=unused-argument
        target: typing.Optional[types.ModuleType] = None,  # pylint: disable=unused-argument
    ) -> typing.Optional[machinery.ModuleSpec]:
        """Return module spec if asked for component module.

        Args:
            fullname: Module full name.
            path: module path (unused).
            target: module target (unused).

        Returns:
            Component module spec or nothing.
        """
        if fullname == self._name:
            LOGGER.debug('Injecting component module loader')
            return machinery.ModuleSpec(fullname, self._loader)
        return None

    @classmethod
    def create(
        cls, module: types.ModuleType, onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None
    ) -> typing.Iterable['Finder']:
        """Return the chain of finders for given module creating fake intermediate modules for missing parents.

        Args:
            module: Module to create a finder for.
            onexec: Optional callback to be registered with given module.

        Returns:
            Sequence of necessary finders.
        """

        def virtualize(name: str) -> bool:
            """Check the given package is a real one (can be imported) or create a virtual instance of it.

            Args:
                name: Package name.

            Returns:
                True if virtualized, otherwise False to stop the walk.
            """
            try:
                importlib.import_module(name)
                return False
            except ModuleNotFoundError:
                level = types.ModuleType(name)
                level.__path__ = [name]  # to make it a package it needs a __path__
                result.append(cls(level))
                return True

        result = []
        if package := _parent(module.__name__):
            _walkup(package, virtualize)
        result.append(cls(module, onexec))
        return tuple(result)


def _parent(name: str) -> typing.Optional[str]:
    """Get parent path of the given module or package path.

    Args:
        name: Module or package path to get the parent for.

    Returns:
        Parent package path.
    """
    return re.match(r'(?:(.*)\.)?(.*)', name).group(1)  # different from module.rsplit('.', 1)


def _walkup(name: str, handler: typing.Callable[[str], bool]) -> None:
    """Walk the module name parents and call the handler for each until it either returns False or the root is
    reached.

    Args:
        name: Qualified module name.
        handler: Function to be called with each parent - returning False signalizes to stop the walk.
    """
    assert name
    if handler(name) and (name := _parent(name)):
        _walkup(name, handler)


def _unload(module: str) -> None:
    """Unload the current module instance and all of its parent modules.

    Args:
        module: to be unloaded.
    """

    def rmmod(level: str) -> bool:
        if level in sys.modules:
            del sys.modules[level]
        return True

    _walkup(module, rmmod)


@contextlib.contextmanager
def context(module: types.ModuleType) -> typing.Iterable[None]:
    """Context manager that adds an importlib.MetaPathFinder to sys._meta_path while in the context faking imports
    of forml.project.component to a virtual fabricated module.

    Args:
        module: Module to be returned upon import of forml.project.component.

    Returns:
        Context manager.
    """
    sys.meta_path[:0] = finders = Finder.create(module)
    name = module.__name__
    _unload(name)
    yield
    finders = set(finders)
    sys.meta_path = [f for f in sys.meta_path if f not in finders]
    _unload(name)


def search(*paths: typing.Union[str, pathlib.Path]) -> None:
    """Simply add the given paths to the front of sys.path removing all potential duplicates.

    Args:
        *paths: Paths to be inserted to sys.path.
    """
    new = []
    for item in itertools.chain((str(pathlib.Path(p).resolve()) for p in paths), sys.path):
        if item not in new:
            new.append(item)
    sys.path = new


@contextlib.contextmanager
def searched(*paths: typing.Union[str, pathlib.Path]) -> typing.Iterable[None]:
    """Context manager for putting given paths on python module search path but only for the duration of the context.

    Args:
        *paths: Paths to be inserted to sys.path when in the context.

    Returns:
        Context manager.
    """
    original = list(sys.path)
    search(*paths)
    yield
    sys.path = original


def isolated(name: str, path: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> types.ModuleType:
    """Import module of given name either under path from isolated environment or virtual.

    Args:
        name: Name of module to be loaded.
        path: Optional path to search indicate path based search.

    Returns:
        Imported module.
    """
    with searched(*([path] if path else [])):
        _unload(name)
        importlib.invalidate_caches()
        module = importlib.import_module(name)
    if not isinstance(module.__loader__, Finder.Loader):
        source = getattr(module, '__file__', None)
        if bool(path) ^ bool(source) or (path and not source.startswith(str(pathlib.Path(path).resolve()))):
            raise ModuleNotFoundError(f'No module named {name}', name=name)
    return module
