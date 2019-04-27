"""
Project component management.
"""
import logging
import sys
import types
import typing

import importlib
from importlib import abc, machinery


LOGGER = logging.getLogger(__name__)

_CACHE: typing.Dict[str, typing.Any] = dict()


def setup(instance: typing.Any) -> None:  # pylint: disable=unused-argument
    """Dummy component setup (merely for the sake of IDE sanity). In real usecase this functions represents
    the Module.setup method.

    Args:
        instance: Component instance to be registered.
    """
    LOGGER.warning('Setup accessed outside of a Context')


class Context:
    """Context manager that adds a importlib.MetaPathFinder to sys._meta_path while in the context faking imports
    of forml.project.component to a ephemeral fabricated module.
    """
    class Finder(abc.MetaPathFinder):
        """Module path finder implementation.
        """
        class Loader(abc.Loader):  # pylint: disable=abstract-method
            """Module loader implementation.
            """
            def __init__(self, module: types.ModuleType):
                self._module: types.ModuleType = module

            def create_module(self, spec) -> types.ModuleType:
                """Return our fake module instance.

                Args:
                    spec: Module spec.

                Returns: Module instance.
                """
                return self._module

            def exec_module(self, module) -> None:
                """Merely a no-ops but required by abc.Loader to be implemented...

                Args:
                    module: to be loaded
                """

        def __init__(self, module: types.ModuleType):
            self._loader: Context.Finder.Loader = self.Loader(module)

        # pylint: disable=unused-argument
        def find_spec(self, fullname: str, path, target) -> typing.Optional[machinery.ModuleSpec]:
            """Return module spec if asked for component module.

            Args:
                fullname: Module full name.
                path: module path (unused).
                target: module target (unused).

            Returns: Component module spec or nothing.
            """
            if fullname == __name__:
                LOGGER.debug('Injecting component module loader')
                return machinery.ModuleSpec(fullname, self._loader)
            return None

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

        self._finder: typing.Optional[Context.Finder] = self.Finder(Module())

    @staticmethod
    def _unload() -> None:
        """Unload the current module instance.
        """
        if __name__ in sys.modules:
            del sys.modules[sys.modules[__name__].__package__]
            del sys.modules[__name__]

    def __enter__(self) -> 'Context':
        sys.meta_path.insert(0, self._finder)
        self._unload()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.meta_path.remove(self._finder)
        self._unload()


def load(module: str, force: typing.Optional[bool] = False) -> typing.Any:
    """Caching component loader.

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
        _CACHE[module] = component

    if module not in _CACHE or force:
        with Context(handler):
            if module in sys.modules:
                if sys.modules[module].__package__ and sys.modules[module].__package__ != module:
                    del sys.modules[sys.modules[module].__package__]
                del sys.modules[module]
            LOGGER.debug('Importing project component from %s', module)
            importlib.import_module(module)

    return _CACHE[module]
