"""Project setup mechanics.
"""
import collections
from collections import abc
import logging
import sys
import types
import typing

from forml.runtime import process

import forml
from forml import etl, conf
from forml.flow.pipeline import topology
from forml.project import component as importer
from forml.runtime.asset import access
from forml.runtime.asset.persistent.registry import virtual

LOGGER = logging.getLogger(__name__)


class Error(forml.Error):
    """Project exception.
    """


class Descriptor(collections.namedtuple('Descriptor', 'source, pipeline, evaluation')):
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
                LOGGER.warning('Incomplete builder (missing %s)', ', '.join(c for c, h in self if not h))
            return Descriptor(*(self._handlers[c].value for c in Descriptor._fields))

    def __new__(cls, source: 'etl.Source', pipeline: topology.Composable,
                evaluation: typing.Optional[topology.Operator] = None):
        if not isinstance(pipeline, topology.Composable):
            raise Error('Invalid pipeline')
        if not isinstance(source, etl.Source):
            raise Error('Invalid source')
        if evaluation and not isinstance(evaluation, topology.Operator):
            raise Error('Invalid evaluation')
        return super().__new__(cls, source, pipeline, evaluation)

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
        if any(c not in builder for c in modules):
            raise Error('Unexpected project component')
        package = f'{package.rstrip(".")}.' if package else ''
        for component, setter in builder:
            mod = modules.get(component) or component
            if '.' not in mod:
                mod = package + mod
            try:
                setter(importer.load(mod))
            except ModuleNotFoundError as err:
                LOGGER.warning('Project %s error: %s', component, err)
        return builder.build()


class Artifact(collections.namedtuple('Artifact', 'path, package, modules')):
    """Project artifact handle.
    """
    class Launcher:
        """Runner proxy class with preconfigured assets to launch given artifact.
        """
        def __init__(self, assets: access.Assets):
            self._assets: access.Assets = assets

        def __getitem__(self, runner: str) -> process.Runner:
            return process.Runner[runner](self._assets)

        def __getattr__(self, mode: str) -> typing.Callable:
            return getattr(process.Runner(self._assets), mode)

    def __new__(cls, path: typing.Optional[str] = None, package: typing.Optional[str] = None, **modules: typing.Any):
        prefix = package or conf.PRJ_NAME
        for key, value in modules.items():
            if not isinstance(value, str):  # component provided as true instance rather then module path
                modules[key] = importer.Virtual(value, f'{prefix}.{key}').path
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

    @property
    def launcher(self) -> 'Artifact.Launcher':
        """Return the launcher configured with a virtual registry preloaded with this artifact.

        Returns: Launcher instance.
        """
        registry = virtual.Registry()
        project = self.package or conf.PRJ_NAME
        registry.push(project, 0, self)
        return self.Launcher(access.Assets(project, registry=registry))
