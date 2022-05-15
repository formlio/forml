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
Project component management.
"""
import collections
import importlib
import inspect
import logging
import os
import pathlib
import secrets
import sys
import types
import typing

import forml
from forml import evaluation, flow
from forml.io import dsl, layout

from .. import _body, _importer
from .._component import virtual

LOGGER = logging.getLogger(__name__)


def setup(instance: typing.Any) -> None:  # pylint: disable=unused-argument
    """Dummy component setup representing the API signature of the fake module injected by load.Component.setup.

    Args:
        instance: Component instance to be registered.
    """
    LOGGER.debug('Setup accessed outside of a Context')


class Source(typing.NamedTuple):
    """Feed independent data source component."""

    extract: 'Source.Extract'
    transform: typing.Optional[flow.Composable] = None

    Labels = typing.Union[
        dsl.Feature,
        typing.Sequence[dsl.Feature],
        flow.Spec[flow.Actor[layout.Tabular, None, tuple[layout.RowMajor, layout.RowMajor]]],
    ]
    """Label type - either single column, multiple columns or generic label extracting actor (with two output ports)."""

    class Extract(collections.namedtuple('Extract', 'train, apply, labels, ordinal')):
        """Combo of select statements for the different modes."""

        train: dsl.Query
        apply: dsl.Query
        labels: typing.Optional['Source.Labels']
        ordinal: typing.Optional[dsl.Operable]

        def __new__(
            cls,
            train: dsl.Queryable,
            apply: dsl.Queryable,
            labels: typing.Optional['Source.Labels'],
            ordinal: typing.Optional[dsl.Operable],
        ):
            train = train.query
            apply = apply.query
            if labels is not None and not isinstance(labels, flow.Spec):
                if isinstance(labels, dsl.Feature):
                    lseq = [labels]
                else:
                    lseq = labels = tuple(labels)
                if {c.operable for c in train.features}.intersection(c.operable for c in lseq):
                    raise forml.InvalidError('Label-feature overlap')
            if train.schema != apply.schema:
                raise forml.InvalidError('Train-apply schema mismatch')
            if ordinal:
                ordinal = dsl.Operable.ensure_is(ordinal)
            return super().__new__(cls, train, apply, labels, ordinal)

    @classmethod
    def query(
        cls,
        features: dsl.Queryable,
        labels: typing.Optional['Source.Labels'] = None,
        apply: typing.Optional[dsl.Queryable] = None,
        ordinal: typing.Optional[dsl.Operable] = None,
    ) -> 'Source':
        """Create new source component with the given parameters. All parameters are the DSL objects - either queries
        or columns.

        Args:
            features: Query defining the train (and if same also the ``apply``) features.
            labels: (Sequence of) training label column(s) or label extraction actor spec.
            apply: Optional query defining the apply features (if different from train ones). If provided, it must
                   result in the same schema as the main provided via ``features``.
            ordinal: Optional specification of an ordinal column.

        Returns:
            Source component instance.
        """
        return cls(cls.Extract(features, apply or features, labels, ordinal))  # pylint: disable=no-member

    def __rshift__(self, transform: flow.Composable) -> 'Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, flow.Composable], **modules: typing.Any) -> '_body.Artifact':
        """Create an artifact from this source and given pipeline.

        Args:
            pipeline: Pipeline to create the artifact with.
            **modules: Other optional artifact modules.

        Returns:
            Project artifact instance.
        """
        return _body.Artifact(source=self, pipeline=pipeline, **modules)


class Evaluation(typing.NamedTuple):
    """Evaluation component."""

    metric: evaluation.Metric
    """Loss/Score function."""

    method: evaluation.Method
    """Strategy for generation validation data - ie holdout, cross-validation etc."""


class Virtual:
    """Virtual component module based on real component instance."""

    def __init__(self, component: typing.Any, package: typing.Optional[str] = None):
        def onexec(_: types.ModuleType) -> None:
            """Module onexec handler that fakes the component registration using the setup() method."""
            LOGGER.debug('Accessing virtual component module')
            getattr(importlib.import_module(__name__), setup.__name__)(component)

        if not package:
            package = secrets.token_urlsafe(16)
        self._path = f'{virtual.__name__}.{package}'
        LOGGER.debug('Registering virtual component [%s]: %s', component, self._path)
        sys.meta_path[:0] = _importer.Finder.create(types.ModuleType(self._path), onexec)

    @property
    def path(self) -> str:
        """The virtual path representing this component.

        Returns:
            Virtual component module path.
        """
        return self._path


def load(module: str, path: typing.Optional[typing.Union[str, pathlib.Path]] = None) -> typing.Any:
    """Component loader.

    Args:
        module: Python module containing the component to be loaded.
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
        expected = module.replace('.', os.path.sep)
        if path:
            expected = os.path.join(path, expected)
        return expected.endswith(actual)

    class Component(types.ModuleType):
        """Fake component module."""

        Source = Source
        Evaluation = Evaluation

        __path__ = globals()['__path__']

        def __init__(self):
            super().__init__(__name__)

        @staticmethod
        def setup(component: typing.Any) -> None:
            """Component module setup handler.

            Args:
                component: Component instance to be registered.
            """
            caller_frame = inspect.currentframe().f_back
            if inspect.getframeinfo(caller_frame).filename != __file__:  # ignore Virtual module setup
                caller_module = inspect.getmodule(caller_frame)
                if caller_module and not is_expected(caller_module.__name__):
                    LOGGER.warning('Ignoring setup from unexpected component of %s', caller_module.__name__)
                    return
            LOGGER.debug('Component setup using %s', component)
            nonlocal result
            if result:
                raise forml.UnexpectedError('Repeated call to component setup')
            result = component

    result = None
    with _importer.context(Component()):
        LOGGER.debug('Importing project component from %s', module)
        _importer.isolated(module, path)

    return result
