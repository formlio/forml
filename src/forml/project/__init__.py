"""Project setup mechanics.
"""
import collections
from collections import abc
import logging
import sys
import types
import typing

import forml
from forml import etl
from forml.flow.pipeline import topology
from forml.project import component as importer

LOGGER = logging.getLogger(__name__)


class Error(forml.Error):
    """Project exception.
    """


class Descriptor(collections.namedtuple('Descriptor', 'source, pipeline')):
    """Top level ForML project descriptor holding the implementations of individual project components.
    """
    class Builder(abc.Set):
        """Descriptor builder allowing to setup attributes one by one.
        """
        class Handler:
            """Simple callable that persists the provided value.
            """
            def __init__(self):
                self.value = None

            def __call__(self, value: typing.Any) -> None:
                self.value = value

            def __bool__(self):
                return self.value is not None

        def __init__(self):
            self._handlers: typing.Mapping[str, Descriptor.Builder.Handler] = {
                c:  self.Handler() for c in Descriptor._fields}

        def __iter__(self) -> typing.Iterator[typing.Tuple[str, typing.Callable[[typing.Any], None]]]:
            for component, handler in self._handlers.items():
                yield component, handler

        def __len__(self):
            return len(self._handlers)

        def __contains__(self, item):
            return item in self._handlers.keys()

        def build(self) -> 'Descriptor':
            """Create the descriptor.

            Returns: Descriptor instance.
            """
            if not all(self._handlers.values()):
                raise Error(f'Incomplete builder (missing {", ".join(c for c, h in self if not h)})')
            return Descriptor(*(self._handlers[c].value for c in Descriptor._fields))

    def __new__(cls, source: etl.Source, pipeline: topology.Composable):
        if not isinstance(pipeline, topology.Composable):
            raise Error('Invalid pipeline')
        if not isinstance(source, etl.Source):
            raise Error('Invalid source')
        return super().__new__(cls, source, pipeline)

    @classmethod
    def load(cls, package: typing.Optional[str] = None, **modules) -> 'Descriptor':
        """Setup the descriptor based on provider package and/or individual modules.

            Either package is provided and all individual modules without dot in their names are considered as
            relative to that package or each module must be specified absolutely.

        Args:
            package: Base package to be considered as a root for all component modules.
            **modules: Component module mappings.
        Returns: Project descriptor.
        """
        builder = cls.Builder()
        if modules.keys() > builder:
            raise Error('Unexpected project component')
        package = f'{package.rstrip(".")}.' if package else ''
        for component, setter in builder:
            mod = modules.get(component) or component
            if '.' not in mod:
                mod = package + mod
            try:
                setter(importer.load(mod))
            except ModuleNotFoundError as err:
                raise Error(f'Project {component} error: {err}')
        return builder.build()


class Artifact(collections.namedtuple('Artifact', 'path, package, modules')):
    """Project artifact handle.
    """
    def __new__(cls, path: typing.Optional[str] = None, package: typing.Optional[str] = None, **modules: str):
        return super().__new__(cls, path, package, types.MappingProxyType(modules))

    def __getnewargs_ex__(self):
        return (self.path, self.package), dict(self.modules)

    @property
    def descriptor(self) -> Descriptor:
        """Extracting the project descriptor from this artifact.

        Returns: Project descriptor.
        """
        if self.path:
            sys.path.insert(0, self.path)
        return Descriptor.load(self.package, **self.modules)
