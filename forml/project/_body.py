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

import forml
from forml import conf, flow
from forml.io import asset

from . import _component, _distribution, _importer

if typing.TYPE_CHECKING:
    from forml import project, runtime

LOGGER = logging.getLogger(__name__)


class Components(collections.namedtuple('Components', 'source, pipeline, evaluation')):
    """Top level ForML project components holding the implementations of individual project components."""

    source: 'project.Source'
    pipeline: flow.Composable
    evaluation: typing.Optional['project.Evaluation']

    class Builder(abc.Set):
        """Descriptor builder allowing to setup attributes one by one."""

        class Handler:
            """Simple callable that persists the provided value."""

            def __init__(self):
                self.value = None

            def __call__(self, value: typing.Any) -> None:
                self.value = value

            def __bool__(self):
                return self.value is not None

        def __init__(self):
            self._handlers: typing.Mapping[str, Components.Builder.Handler] = {
                c: self.Handler() for c in Components._fields
            }

        def __iter__(self) -> typing.Iterator[tuple[str, typing.Callable[[typing.Any], None]]]:
            yield from self._handlers.items()

        def __len__(self):
            return len(self._handlers)

        def __contains__(self, item):
            return item in self._handlers.keys()

        def build(self) -> 'Components':
            """Create the components.

            Returns:
                Descriptor instance.
            """
            if not all(self._handlers.values()):
                LOGGER.debug('Incomplete builder (missing %s)', ', '.join(c for c, h in self if not h))
            return Components(*(self._handlers[c].value for c in Components._fields))

    def __new__(
        cls,
        source: 'project.Source',
        pipeline: flow.Composable,
        evaluation: typing.Optional['project.Evaluation'] = None,
    ):
        if not isinstance(pipeline, flow.Composable):
            raise forml.InvalidError('Invalid pipeline')
        if not isinstance(source, _component.Source):
            raise forml.InvalidError('Invalid source')
        if evaluation and not isinstance(evaluation, _component.Evaluation):
            raise forml.InvalidError('Invalid evaluation')
        return super().__new__(cls, source, pipeline, evaluation)

    @classmethod
    def load(
        cls,
        package: typing.Optional[str] = None,
        path: typing.Optional[typing.Union[str, pathlib.Path]] = None,
        **modules,
    ) -> 'Components':
        """Setup the components based on provider package and/or individual modules.

            Either package is provided and all individual modules without dot in their names are considered as
            relative to that package or each module must be specified absolutely.

        Args:
            path: Path to load from.
            package: Base package to be considered as a root for all component modules.
            **modules: Component module mappings.

        Returns:
            Project components.
        """
        builder = cls.Builder()
        if any(c not in builder for c in modules):
            raise forml.UnexpectedError('Unexpected project component')
        package = f'{package.rstrip(".")}.' if package else ''
        for component, setter in builder:
            mod = modules.get(component) or component
            if '.' not in mod:
                mod = package + mod
            try:
                setter(_component.load(mod, path))
            except ModuleNotFoundError as err:
                if not mod.startswith(err.name):
                    raise err
                LOGGER.debug('Component %s not found', component)
        return builder.build()


class Artifact(collections.namedtuple('Artifact', 'path, package, modules')):
    """Project artifact handle."""

    def __new__(
        cls,
        path: typing.Optional[typing.Union[str, pathlib.Path]] = None,
        package: typing.Optional[str] = None,
        **modules: typing.Any,
    ):
        if path:
            path = pathlib.Path(path).resolve()
        prefix = package or conf.PRJNAME
        for key, value in modules.items():
            if not isinstance(value, str):  # component provided as true instance rather than module path
                modules[key] = _component.Virtual(value, f'{prefix}.{key}').path
        return super().__new__(cls, path, package, types.MappingProxyType(modules))

    def __getnewargs_ex__(self):
        return (self.path, self.package), dict(self.modules)

    def __hash__(self):
        return hash(self.path) ^ hash(self.package) ^ hash(tuple(sorted(self.modules.items())))

    @property
    def components(self) -> Components:
        """Extracting the project components from this artifact.

        Returns:
            Project components.
        """
        return Components.load(self.package, self.path, **self.modules)

    @functools.cached_property
    def launcher(self) -> 'runtime.Virtual':
        """Return the launcher configured with a virtual registry preloaded with this artifact.

        Returns:
            Launcher instance.
        """

        class Manifest(types.ModuleType):
            """Fake manifest module."""

            NAME = (self.package or conf.PRJNAME).replace('.', '-')
            VERSION = '0'
            PACKAGE = self.package
            MODULES = self.modules

            def __init__(self):
                super().__init__(_distribution.Manifest.MODULE)

        from forml import runtime  # pylint: disable=import-outside-toplevel

        with _importer.context(Manifest()):
            # dummy package forced to load our fake manifest
            return runtime.Virtual(_distribution.Package(self.path or asset.mkdtemp(prefix='dummy-')))
