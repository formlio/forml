"""
Project component management.
"""
import importlib
import logging
import pathlib
import secrets
import sys
import types
import typing

from forml.project import importer
from forml.project.component import virtual

LOGGER = logging.getLogger(__name__)


def setup(instance: typing.Any) -> None:  # pylint: disable=unused-argument
    """Dummy component setup (merely for the sake of IDE sanity). In real usecase this functions represents
    the Module.setup method.

    Args:
        instance: Component instance to be registered.
    """
    LOGGER.warning('Setup accessed outside of a Context')


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
        self._path = f'{virtual.__name__}.{package}'
        LOGGER.debug('Registering virtual component [%s]: %s', component, self._path)
        sys.meta_path[:0] = importer.Finder.create(types.ModuleType(self._path), onexec)

    @property
    def path(self) -> str:
        """The virtual path representing this component.

        Returns: Virtual component module path.
        """
        return self._path


def load(module: str, path: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> typing.Any:
    """Component loader.

    Args:
        module: Python module containing the component to be loaded.
        path: Path to import from.

    Returns: Component instance.
    """
    class Component(types.ModuleType):
        """Fake component module.
        """
        def __init__(self):
            super().__init__(__name__)

        @staticmethod
        def setup(component: typing.Any) -> None:
            """Component module setup handler.

            Args:
                component: Component instance to be registered.
            """
            LOGGER.debug('Component setup using %s', component)
            nonlocal result
            result = component

    result = None
    with importer.context(Component()):
        if module in sys.modules:
            del sys.modules[module]
        LOGGER.debug('Importing project component from %s', module)
        importer.isolated(module, path)

    return result
