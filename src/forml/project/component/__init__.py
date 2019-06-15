"""
Project component management.
"""
import logging
import secrets
import sys
import types
import typing

import importlib
from importlib import abc, machinery

from forml.project.component import virtual


LOGGER = logging.getLogger(__name__)


def setup(instance: typing.Any) -> None:  # pylint: disable=unused-argument
    """Dummy component setup (merely for the sake of IDE sanity). In real usecase this functions represents
    the Module.setup method.

    Args:
        instance: Component instance to be registered.
    """
    LOGGER.warning('Setup accessed outside of a Context')


class Finder(abc.MetaPathFinder):
    """Module path finder implementation.
    """
    class Loader(abc.Loader):  # pylint: disable=abstract-method
        """Module loader implementation.
        """
        def __init__(self, module: types.ModuleType,
                     onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None):
            self._module: types.ModuleType = module
            self._onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = onexec

        def create_module(self, spec) -> types.ModuleType:
            """Return our fake module instance.

            Args:
                spec: Module spec.

            Returns: Module instance.
            """
            return self._module

        def exec_module(self, module: types.ModuleType) -> None:
            """Here we cal the optional onexec handler.

            Args:
                module: to be loaded
            """
            if self._onexec:
                self._onexec(module)

    def __init__(self, name: str, module: types.ModuleType,
                 onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None):
        self._name: str = name
        self._loader: abc.Loader = self.Loader(module, onexec)

    # pylint: disable=unused-argument
    def find_spec(self, fullname: str, path, target) -> typing.Optional[machinery.ModuleSpec]:
        """Return module spec if asked for component module.

        Args:
            fullname: Module full name.
            path: module path (unused).
            target: module target (unused).

        Returns: Component module spec or nothing.
        """
        if fullname == self._name:
            LOGGER.debug('Injecting component module loader')
            return machinery.ModuleSpec(fullname, self._loader)
        return None


class Context:
    """Context manager that adds a importlib.MetaPathFinder to sys._meta_path while in the context faking imports
    of forml.project.component to a virtual fabricated module.
    """
    def __init__(self, handler: typing.Callable[[typing.Any], None]):
        class Module(types.ModuleType):
            """Fake component module.
            """
            def __init__(self):
                super().__init__(__name__)

            @staticmethod
            def setup(instance: typing.Any) -> None:
                """Component module setup handler.

                Args:
                    instance: Component instance to be registered.
                """
                LOGGER.debug('Component setup using %s', instance)
                handler(instance)

        self._finder: Finder = Finder(__name__, Module())

    @staticmethod
    def _unload() -> None:
        """Unload the current module instance.
        """
        for mod in {__name__, __name__.rsplit('.', 1)[0]}:
            if mod in sys.modules:
                del sys.modules[mod]

    def __enter__(self) -> 'Context':
        sys.meta_path.insert(0, self._finder)
        self._unload()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.meta_path.remove(self._finder)
        self._unload()


class Virtual:
    """Virtual component module based on real component instance.
    """
    def __init__(self, component: typing.Any, package: typing.Optional[str] = None):
        def onexec(_: types.ModuleType) -> None:
            """Module onexec handler that fakes the component registration using the setup() method.
            """
            LOGGER.debug('Accessing virtual component module')
            getattr(importlib.import_module(__name__), setup.__name__)(component)

        if not package:
            package = secrets.token_urlsafe(16)
        *parents, module = package.split('.')
        path = virtual.__name__
        for subpath in parents:
            path = f'{path}.{subpath}'
            try:
                importlib.import_module(path)
            except ModuleNotFoundError:
                subpkg = types.ModuleType(path)
                subpkg.__path__ = path  # to make it a package it needs a __path__
                sys.meta_path.insert(0, Finder(path, subpkg))
        self._path: str = f'{path}.{module}'
        LOGGER.debug('Registering virtual component [%s]: %s', component, self._path)
        sys.meta_path.insert(0, Finder(self._path, types.ModuleType(self.path), onexec))

    @property
    def path(self) -> str:
        """The virtual path representing this component.

        Returns: Virtual component module path.
        """
        return self._path


def load(module: str) -> typing.Any:
    """Component loader.

    Args:
        module: Python module containing the component to be loaded.
        force: Reimport the component even if already cached.

    Returns: Component instance.
    """
    def handler(component: typing.Any) -> None:
        """Loader callback.

        Args:
            component: Expected component instance.
        """
        nonlocal result
        result = component

    result = None
    with Context(handler):
        if module in sys.modules:
            del sys.modules[module]
        LOGGER.debug('Importing project component from %s', module)
        importlib.import_module(module)

    return result
