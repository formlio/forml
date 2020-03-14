"""
Project importer machinery.
"""
import contextlib
import importlib
import logging
import pathlib
import re
import sys
import types
import typing
from importlib import abc, machinery

LOGGER = logging.getLogger(__name__)


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

    def __init__(self, module: types.ModuleType,
                 onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None):
        self._name: str = module.__name__
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

    @classmethod
    def create(cls, module: types.ModuleType,
               onexec: typing.Optional[typing.Callable[[types.ModuleType], None]] = None) -> typing.Iterable['Finder']:
        """Return the chain of finders for given module creating fake intermediate modules for missing parents.

        Args:
            module: Module to create a finder for.
            onexec: Optional callback to be registered with given module.

        Returns: Sequence of necessary finders.
        """
        result = list()
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
def context(module: types.ModuleType) -> None:
    """Context manager that adds a importlib.MetaPathFinder to sys._meta_path while in the context faking imports
    of forml.project.component to a virtual fabricated module.
    """
    def unload() -> None:
        """Unload the current module instance and all of its parent modules.
        """
        name = module.__name__
        while name:
            if name in sys.modules:
                del sys.modules[name]
            name, _ = re.match(r'(?:(.*)\.)?(.*)', name).groups()

    original = list(sys.meta_path)
    sys.meta_path[:0] = Finder.create(module)
    unload()
    yield
    sys.meta_path = original
    unload()


def isolated(name: str, path: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> types.ModuleType:
    """Import module of given name either under path from isolated environment or virtual.

    Args:
        name: Name of module to be loaded.
        path: Optional path to search indicate path based search.

    Returns: Imported module.
    """
    original = list(sys.path)
    if path:
        path = str(pathlib.Path(path).resolve())
        sys.path.insert(0, path)
    if name in sys.modules:
        del sys.modules[name]
    importlib.invalidate_caches()
    try:
        module = importlib.import_module(name)
    finally:
        sys.path = original
    if not isinstance(module.__loader__, Finder.Loader):
        source = getattr(module, '__file__', None)
        if bool(path) ^ bool(source) or (path and not source.startswith(path)):
            raise ModuleNotFoundError(f'No module named {name}')
    return module
