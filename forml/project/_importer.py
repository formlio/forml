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

        def create_module(self, spec) -> types.ModuleType:
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

    # pylint: disable=unused-argument
    def find_spec(self, fullname: str, path, target) -> typing.Optional[machinery.ModuleSpec]:
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
        result = []
        *parents, _ = module.__name__.split('.')
        path = ''
        for subpath in parents:
            path += subpath
            try:
                importlib.import_module(path)
            except ModuleNotFoundError:
                subpkg = types.ModuleType(path)
                subpkg.__path__ = path  # to make it a package it needs a __path__
                result.append(cls(subpkg))
            path += '.'
        result.append(cls(module, onexec))
        return tuple(result)


@contextlib.contextmanager
def context(module: types.ModuleType) -> typing.Iterable[None]:
    """Context manager that adds a importlib.MetaPathFinder to sys._meta_path while in the context faking imports
    of forml.project.component to a virtual fabricated module.

    Args:
        module: Module to be returned upon import of forml.project.component.

    Returns:
        Context manager.
    """

    def unload() -> None:
        """Unload the current module instance and all of its parent modules."""
        name = module.__name__
        while name:
            if name in sys.modules:
                del sys.modules[name]
            name, _ = re.match(r'(?:(.*)\.)?(.*)', name).groups()  # different from name.rsplit('.', 1)

    sys.meta_path[:0] = finders = Finder.create(module)
    unload()
    yield
    finders = set(finders)
    sys.meta_path = [f for f in sys.meta_path if f not in finders]
    unload()


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
        if name in sys.modules:
            del sys.modules[name]
        importlib.invalidate_caches()
        module = importlib.import_module(name)
    if not isinstance(module.__loader__, Finder.Loader):
        source = getattr(module, '__file__', None)
        if bool(path) ^ bool(source) or (path and not source.startswith(str(pathlib.Path(path).resolve()))):
            raise ModuleNotFoundError(f'No module named {name}')
    return module
