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
import inspect
import itertools
import logging
import os
import pathlib
import re
import sys
import types
import typing
import warnings
from importlib import abc, machinery

import forml

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
            """Here we call the optional onexec handler.

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
        """Return the chain of finders for given module creating fake intermediate modules for
        missing parents.

        Args:
            module: Module to create a finder for.
            onexec: Optional callback to be registered with given module.

        Returns:
            Sequence of necessary finders.
        """

        def virtualize(name: str) -> bool:
            """Check the given package is a real one (can be imported) or create a virtual instance
            of it.

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
    """Walk the module name parents and call the handler for each until it either returns False or
    the root is reached.

    Args:
        name: Qualified module name.
        handler: Function to be called with each parent - returning False signalizes to stop the
                 walk.
    """
    assert name
    if handler(name) and (name := _parent(name)):
        _walkup(name, handler)


@contextlib.contextmanager
def _unloaded(name: str) -> typing.Iterable[None]:
    """Unload the current module instance and all of its parent modules.

    Args:
        name: Module to be unloaded.
    """

    def rmmod(level: str) -> bool:
        if level in sys.modules:
            original[level] = sys.modules[level]
            del sys.modules[level]
        return True

    original = {}
    _walkup(name, rmmod)
    yield
    sys.modules.update(original)


def search(*paths: typing.Union[str, pathlib.Path]) -> None:
    """Simply add the given paths to the front of ``sys.path`` removing all potential duplicates.

    Args:
        *paths: Paths to be inserted to ``sys.path``.
    """
    new = []
    for item in itertools.chain((str(pathlib.Path(p).resolve()) for p in paths), sys.path):
        if item not in new:
            new.append(item)
    sys.path = new


@contextlib.contextmanager
def _searched(*paths: typing.Union[str, pathlib.Path]) -> typing.Iterable[None]:
    """Context manager for putting given paths on python module search path but only for the
    duration of the context.

    Args:
        *paths: Paths to be inserted to ``sys.path`` when in the context.

    Returns:
        Context manager.
    """
    original = list(sys.path)
    search(*paths)
    yield
    sys.path = original


def isolated(name: str, path: typing.Optional[typing.Union[str, pathlib.Path]]) -> types.ModuleType:
    """Import module of given name either under path from isolated environment or virtual.

    Args:
        name: Name of module to be loaded.
        path: Path to search indicate path based search.

    Returns:
        Imported module.
    """
    with _unloaded(name), _searched(*([path] if path else [])):
        importlib.invalidate_caches()
        module = importlib.import_module(name)
    if path and not isinstance(module.__loader__, Finder.Loader):
        source = getattr(module, '__file__', None)
        if not source or not source.startswith(str(pathlib.Path(path).resolve())):
            raise ModuleNotFoundError(f'No module named {name}', name=name)
    return module


@contextlib.contextmanager
def context(module: types.ModuleType) -> typing.Iterable[None]:
    """Context manager that adds an ``importlib.MetaPathFinder`` to ``sys._meta_path`` while in the
    context forcing imports of the given module to return the provided instance.

    Args:
        module: Module to be statically returned upon its import attempt.

    Returns:
        Context manager.
    """
    sys.meta_path[:0] = finders = Finder.create(module)
    with _unloaded(module.__name__):
        yield
        finders = set(finders)
        sys.meta_path = [f for f in sys.meta_path if f not in finders]
        if module.__name__ in sys.modules:
            del sys.modules[module.__name__]


def load(
    name: str, entrypoint: typing.Callable[..., None], path: typing.Optional[typing.Union[str, pathlib.Path]] = None
) -> typing.Any:
    """Component loader.

    Args:
        name: Python module containing the component to be loaded.
        entrypoint: Expected callback to be used within the module that we will mimic and capture.
        path: Path to import from.

    Returns:
        Component instance.
    """

    def is_expected(actual: str) -> bool:
        """Test the actually loaded module is the one that's been requested.

        Args:
            actual: Name of the actually loaded module.

        Returns:
            True if the actually loaded module is the one expected.
        """
        actual = actual.replace('.', os.path.sep)
        expected = name.replace('.', os.path.sep)
        if path:
            expected = os.path.join(path, expected)
        return expected.endswith(actual)

    def patched(component: typing.Any) -> None:
        """Patched entrypoint handler.

        Args:
            component: Component instance to be registered.
        """
        nonlocal called, result
        if called:
            raise forml.UnexpectedError('Repeated call to component setup')
        called = True
        caller_module = inspect.currentframe().f_back.f_locals['__name__']
        if not is_expected(caller_module):
            warnings.warn(f'Ignoring setup from unexpected component of {caller_module}')
            return
        LOGGER.debug('Component setup using %s', component)
        result = component

    entrypoint_original = importlib.import_module(entrypoint.__module__)
    entrypoint_patched = types.ModuleType(entrypoint.__module__)
    for item in dir(entrypoint_original):
        setattr(entrypoint_patched, item, getattr(entrypoint_original, item))
    setattr(entrypoint_patched, entrypoint.__name__, patched)
    called = False
    result = None
    with context(entrypoint_patched):
        LOGGER.debug('Importing project component from %s', name)
        isolated(name, path)
    if not called:
        raise forml.InvalidError(f'Component setup incomplete: {name}')
    return result
