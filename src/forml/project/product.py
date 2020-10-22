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

"""Project product reference.
"""
import collections
import functools
import logging
import pathlib
import types
import typing
from collections import abc

from forml import conf, error, runtime, io
from forml.conf import provider as provcfg
from forml.flow.pipeline import topology
from forml.io import etl
from forml.lib.registry import virtual
from forml.project import component as compmod, distribution, importer
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import root

LOGGER = logging.getLogger(__name__)


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
                c: self.Handler() for c in Descriptor._fields}

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
            raise error.Invalid('Invalid pipeline')
        if not isinstance(source, etl.Source):
            raise error.Invalid('Invalid source')
        if evaluation and not isinstance(evaluation, topology.Operator):
            raise error.Invalid('Invalid evaluation')
        return super().__new__(cls, source, pipeline, evaluation)

    @classmethod
    def load(cls, package: typing.Optional[str] = None,
             path: typing.Optional[typing.Union[str, pathlib.Path]] = None, **modules) -> 'Descriptor':
        """Setup the descriptor based on provider package and/or individual modules.

            Either package is provided and all individual modules without dot in their names are considered as
            relative to that package or each module must be specified absolutely.

        Args:
            path: Path to load from.
            package: Base package to be considered as a root for all component modules.
            **modules: Component module mappings.
        Returns: Project descriptor.
        """
        builder = cls.Builder()
        if any(c not in builder for c in modules):
            raise error.Unexpected('Unexpected project component')
        package = f'{package.rstrip(".")}.' if package else ''
        for component, setter in builder:
            mod = modules.get(component) or component
            if '.' not in mod:
                mod = package + mod
            try:
                setter(compmod.load(mod, path))
            except ModuleNotFoundError as err:
                LOGGER.warning('Project %s error: %s', component, err)
        return builder.build()


class Artifact(collections.namedtuple('Artifact', 'path, package, modules')):
    """Project artifact handle.
    """
    class Launcher:
        """Runner proxy class with preconfigured assets to launch given artifact.
        """
        def __init__(self, registry: persistent.Registry, project: str):
            self._registry: persistent.Registry = registry
            self._project: str = project

        def __call__(self, runner: typing.Optional[provcfg.Runner] = None,
                     feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, io.Feed]]] = None,
                     sink: typing.Optional[typing.Union[
                         provcfg.Sink.Mode, str, io.Sink]] = None) -> 'runtime.Platform.Runner':
            return runtime.Platform(runner, self._registry, feeds, sink).runner(self._project)

        def __getitem__(self, runner: str) -> 'runtime.Platform.Runner':
            return self(provcfg.Runner.resolve(runner))

        def __getattr__(self, mode: str) -> typing.Callable:
            return getattr(self(), mode)

    def __new__(cls, path: typing.Optional[typing.Union[str, pathlib.Path]] = None,
                package: typing.Optional[str] = None, **modules: typing.Any):
        if path:
            path = pathlib.Path(path).resolve()
        prefix = package or conf.PRJNAME
        for key, value in modules.items():
            if not isinstance(value, str):  # component provided as true instance rather then module path
                modules[key] = compmod.Virtual(value, f'{prefix}.{key}').path
        return super().__new__(cls, path, package, types.MappingProxyType(modules))

    def __getnewargs_ex__(self):
        return (self.path, self.package), dict(self.modules)

    def __hash__(self):
        return hash(self.path) ^ hash(self.package) ^ hash(tuple(sorted(self.modules.items())))

    @property
    def descriptor(self) -> Descriptor:
        """Extracting the project descriptor from this artifact.

        Returns: Project descriptor.
        """
        return Descriptor.load(self.package, self.path, **self.modules)

    @property
    @functools.lru_cache()
    def launcher(self) -> 'Artifact.Launcher':
        """Return the launcher configured with a virtual registry preloaded with this artifact.

        Returns: Launcher instance.
        """
        project = (self.package or conf.PRJNAME).replace('.', '-')

        class Manifest(types.ModuleType):
            """Fake manifest module.
            """
            NAME = project
            VERSION = '0'
            PACKAGE = self.package
            MODULES = self.modules

            def __init__(self):
                super().__init__(distribution.Manifest.MODULE)

        with importer.context(Manifest()):
            # dummy package forced to load our fake manifest
            package = distribution.Package(self.path or persistent.mkdtemp(prefix='dummy-'))
        registry = virtual.Registry()
        root.Level(registry).get(project).put(package)
        return self.Launcher(registry, project)
