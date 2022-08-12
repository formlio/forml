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
from forml import flow
from forml.io import dsl, layout

from .. import _body, _importer
from .._component import virtual

if typing.TYPE_CHECKING:
    from forml import evaluation, project

LOGGER = logging.getLogger(__name__)


def setup(instance: typing.Any) -> None:  # pylint: disable=unused-argument
    """Interface for registering principal component instances.

    This function is expected to be called exactly once from within every component module passing
    the component instance.

    The true implementation of this function is only provided when imported within the *component
    loader context* (outside the context this is effectively no-op).

    Args:
        instance: Principal component instance to be registered.
    """
    LOGGER.debug('Principal component setup attempted outside of a loader context: %s', instance)


class Source(typing.NamedTuple):
    """ForML data-source descriptor representing the ETL operation to be carried out at runtime
    to deliver the required input payload to the project pipeline.

    The descriptor is combination of an *extraction* DSL query and an optional *transformation*
    workflow.

    Attention:
        Instances are supposed to be created using the :meth:`query` method rather than calling the
        constructor directly.
    """

    extract: 'project.Source.Extract'
    """A DSL query to be performed by the eventual platform Feed representing the *extraction*
    part of the ETL process. The value is assembled directly from the parameters of the ``.query()``
    method."""
    transform: typing.Optional['flow.Composable'] = None
    """A workflow to be expanded into a regular task graph representing the optional
    *transformation* part of the ETL process. The value is accrued from (potentially repeated)
    chaining of the Source instance with workflow *operators* using the ``>>`` composition-like
    syntax.

    Examples:
        >>> ETL = project.Source.query(
        ...     schema.FooBar.select(schema.FooBar.foo)
        ... ) >> payload.ToPandas()
    """

    Labels = typing.Union[
        dsl.Feature,
        typing.Sequence[dsl.Feature],
        flow.Builder[flow.Actor[layout.Tabular, None, tuple[layout.RowMajor, layout.RowMajor]]],
    ]
    """Label type - either a single column, multiple columns or a generic label extracting actor
    (with two output ports).
    """

    class Extract(collections.namedtuple('Extract', 'train, apply, labels, ordinal')):
        """Combo of select statements for the different modes."""

        train: dsl.Statement
        apply: dsl.Statement
        labels: typing.Optional['project.Source.Labels']
        ordinal: typing.Optional[dsl.Operable]

        def __new__(
            cls,
            train: dsl.Source,
            apply: dsl.Source,
            labels: typing.Optional['project.Source.Labels'],
            ordinal: typing.Optional[dsl.Operable],
        ):
            train = train.statement
            apply = apply.statement
            if labels is not None and not isinstance(labels, flow.Builder):
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
        features: dsl.Source,
        labels: typing.Optional['project.Source.Labels'] = None,
        apply: typing.Optional[dsl.Source] = None,
        ordinal: typing.Optional[dsl.Operable] = None,
    ) -> 'project.Source':
        """Factory method for creating a new Source descriptor instance with the given *extraction*
        parameters.

        Args:
            features: A DSL query defining the *train-mode* (and implicitly also the *apply-mode*)
                      dataset. The features must not contain any columns specified in the ``labels``
                      parameter.
            labels: Training label (or a sequence of) column(s) or a label extraction actor builder
                    (single input and two output ports of *[features, labels]*).
            apply: Optional query defining the explicit *apply-mode* features (if different from
                   the train ones). If provided, it must result in the same layout as the main one
                   provided via ``features``.
            ordinal: Optional specification of an *ordinal* column defining the relative ordering of
                     the data records. If provided, the workflow can be launched with optional
                     ``lower`` and/or ``upper`` parameters specifying the requested data range.

        Returns:
            Source component instance.
        """
        return cls(cls.Extract(features, apply or features, labels, ordinal))  # pylint: disable=no-member

    def __rshift__(self, transform: 'flow.Composable') -> 'project.Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, 'flow.Composable'], **modules: typing.Any) -> 'project.Artifact':
        """Create a virtual *project handle* from this *Source* and the given *pipeline* component.

        The typical use-case is :doc:`interactive <interactive>` execution.

        Args:
            pipeline: Pipeline component to create the virtual project handle from.
            modules: Optional modules representing the other project components.

        Returns:
            Virtual project handle.

        Examples:
            >>> PIPELINE = payload.ToPandas()
            >>> SOURCE = project.Source.query(
            ...     schema.FooBar.select(schema.FooBar.foo)
            ... )
            >>> SOURCE.bind(PIPELINE).launcher.apply()
        """
        return _body.Artifact(source=self, pipeline=pipeline, **modules)


class Evaluation(typing.NamedTuple):
    """Evaluation component descriptor representing the evaluation configuration.

    Args:
        metric: Loss/Score function to be used to quantify the prediction quality.
        method: Strategy for generating data for the development train-test evaluation (e.g.
                *holdout* or *cross-validation*, etc).

    Examples:
        >>> EVALUATION = project.Evaluation(
        ...     evaluation.Function(sklearn.metrics.log_loss),
        ...     evaluation.HoldOut(test_size=0.2, stratify=True, random_state=42),
        ... )
    """

    metric: 'evaluation.Metric'
    """Loss/Score function to be used to quantify the prediction quality."""

    method: 'evaluation.Method'
    """Strategy for generating data for the development train-test evaluation. """


class Virtual:
    """Virtual component module based on a real component instance."""

    def __init__(self, component: typing.Any, package: typing.Optional[str] = None):
        def onexec(_: types.ModuleType) -> None:
            """Module onexec handler that fakes the component registration using the setup()
            method.
            """
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
