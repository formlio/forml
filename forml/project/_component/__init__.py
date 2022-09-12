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
import enum
import functools
import importlib
import logging
import operator
import secrets
import sys
import types
import typing

import forml
from forml import flow as flowmod
from forml import setup as setupmod
from forml.io import dsl as dslmod
from forml.io import layout

from .. import _body
from . import virtual

if typing.TYPE_CHECKING:
    from forml import evaluation, flow, project  # pylint: disable=reimported
    from forml.io import dsl  # pylint: disable=reimported

LOGGER = logging.getLogger(__name__)


@typing.overload
def setup(source: 'project.Source') -> None:
    """Source component setup entrypoint.

    Args:
        source: Source descriptor.
    """


@typing.overload
def setup(pipeline: 'flow.Composable', schema: 'typing.Optional[dsl.Source.Schema]' = None) -> None:
    """Pipeline component setup entrypoint.

    Args:
        pipeline: Workflow expression.
        schema: Optional schema of the pipeline output.
    """


@typing.overload
def setup(evaluation: 'project.Evaluation') -> None:
    """Evaluation component setup entrypoint.

    Args:
        evaluation: Evaluation descriptor.
    """


def setup(component) -> None:  # pylint: disable=unused-argument
    """Interface for registering principal component instances.

    This function is expected to be called exactly once from within every component module passing
    the component instance.

    The true implementation of this function is only provided when imported within the *component
    loader context* (outside the context this is effectively no-op).

    Args:
        source: Source descriptor.
        pipeline: Workflow expression.
        schema: Optional schema of the pipeline output.
        evaluation: Evaluation descriptor.
    """
    LOGGER.debug('Principal component setup attempted outside of a loader context: %s', component)


class Source(typing.NamedTuple):
    """ForML data source descriptor representing the ETL operation to be carried out at runtime
    to deliver the required input payload to the project pipeline.

    The descriptor is a combination of an *extraction* DSL query and an optional *transformation*
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
        dslmod.Feature,
        typing.Sequence[dslmod.Feature],
        flowmod.Builder[flowmod.Actor[layout.Tabular, None, tuple[layout.RowMajor, layout.RowMajor]]],
    ]
    """Label type - either a single column, multiple columns, or a generic label extracting actor
    (with two output ports) builder.
    """

    class Extract(collections.namedtuple('Extract', 'train, apply, labels, ordinal')):
        """Combo of select statements for the different modes."""

        class Ordinal(collections.namedtuple('Ordinal', 'column, once')):
            """Ordinal specs."""

            @enum.unique
            class Once(enum.Enum):
                """Delivery guarantees semantic for the ordinal column in case of incremental
                querying.
                """

                _ignore_ = 'Bounds'  # pylint: disable=invalid-name

                class Bounds(collections.namedtuple('Bounds', 'lower, upper')):
                    """Upper/lower bound operators."""

                    lower: typing.Callable[['dsl.Operable', 'dsl.Native'], 'dsl.Predicate']
                    upper: typing.Callable[['dsl.Operable', 'dsl.Native'], 'dsl.Predicate']

                EXACTLY = Bounds(operator.ge, operator.lt)
                """Include the lower bound but leave the upper bound out for the next batch."""
                ATMOST = Bounds(operator.gt, operator.le)
                """Leave out the lower bound and include the upper end."""
                ATLEAST = Bounds(operator.ge, operator.le)
                """Include both ends."""

                def __repr__(self):
                    return self.name.lower()

                @classmethod
                def _missing_(cls, value: typing.Any):
                    if isinstance(value, str):
                        value = value.lower()
                        if value in {'most', 'atmost', 'at-most', 'atmostonce', 'at-most-once'}:
                            return cls.ATMOST
                        if value in {'least', 'atleast', 'at-least', 'atleastonce', 'at-least-once'}:
                            return cls.ATLEAST
                        if value in {'exact', 'exactlyonce', 'exactly-once'}:
                            return cls.EXACTLY
                    return super()._missing_(value)

            column: 'dsl.Operable'
            once: 'project.Source.Extract.Ordinal.Once'

            def __new__(
                cls,
                column: 'dsl.Operable',
                once: typing.Optional[typing.Union[str, 'project.Source.Extract.Ordinal.Once']],
            ):
                return super().__new__(
                    cls, dslmod.Operable.ensure_is(column), cls.Once(once) if once else cls.Once.ATLEAST
                )

            def where(
                self, lower: typing.Optional['dsl.Native'], upper: typing.Optional['dsl.Native']
            ) -> typing.Optional['dsl.Predicate']:
                """Construct a DSL predicate using this ordinal specs and the provided bounds.

                Args:
                    lower: Lower ordinal bound.
                    upper: Upper ordinal bound.

                Returns:
                    DSL predicate if lower and/or upper are provided else None.
                """
                terms = []
                if lower is not None:
                    terms.append(self.once.value.lower(self.column, lower))
                if upper is not None:
                    terms.append(self.once.value.upper(self.column, upper))
                return functools.reduce(operator.and_, terms) if terms else None

        train: 'dsl.Statement'
        apply: 'dsl.Statement'
        labels: typing.Optional['project.Source.Labels']
        ordinal: typing.Optional['project.Source.Extract.Ordinal']

        def __new__(
            cls,
            train: 'dsl.Source',
            apply: 'dsl.Source',
            labels: typing.Optional['project.Source.Labels'],
            ordinal: typing.Optional['dsl.Operable'],
            once: typing.Optional[typing.Union[str, 'project.Source.Extract.Ordinal.Once']],
        ):
            train = train.statement
            apply = apply.statement
            if labels is not None and not isinstance(labels, flowmod.Builder):
                if isinstance(labels, dslmod.Feature):
                    lseq = [labels]
                else:
                    lseq = labels = tuple(labels)
                if {c.operable for c in train.features}.intersection(c.operable for c in lseq):
                    raise forml.InvalidError('Label-feature overlap')
            if train.schema != apply.schema:
                raise forml.InvalidError('Train-apply schema mismatch')
            if ordinal:
                ordinal = cls.Ordinal(ordinal, once)
            elif once:
                raise forml.InvalidError('Once without an Ordinal')
            return super().__new__(cls, train, apply, labels, ordinal)

    @classmethod
    def query(
        cls,
        features: 'dsl.Source',
        labels: typing.Optional['project.Source.Labels'] = None,
        apply: typing.Optional['dsl.Source'] = None,
        ordinal: typing.Optional['dsl.Operable'] = None,
        once: typing.Optional[str] = None,
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
            once: The ordinal delivery semantic for *incremental querying*.
                  Possible values are:

                  * ``atleast``: Include both the lower and the upper ordinal bounds (leads to
                    duplicate processing).
                  * ``atmost``: Leave out the lower bound and include the upper one (leads to data
                    loss in case of continuous ordinals - safe for discrete values).
                  * ``exactly``: Include the lower bound but leave the upper bound out for the next
                    batch (excludes processing of the tail records).

        Returns:
            Source component instance.
        """
        return cls(cls.Extract(features, apply or features, labels, ordinal, once))  # pylint: disable=no-member

    def __rshift__(self, transform: 'flow.Composable') -> 'project.Source':
        return self.__class__(self.extract, self.transform >> transform if self.transform else transform)

    def bind(self, pipeline: typing.Union[str, 'flow.Composable'], **modules: typing.Any) -> 'project.Artifact':
        """Create a virtual *project handle* from this *Source* and the given *pipeline* component.

        The typical use case is the :doc:`interactive <interactive>` execution.

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

    def __init__(
        self,
        component: typing.Any,
        package: typing.Optional[str] = None,
        entrypoint: typing.Callable[..., None] = setup,
    ):
        def onexec(_: types.ModuleType) -> None:
            """Module onexec handler that fakes the component registration using the ``entrypoint``
            method.
            """
            LOGGER.debug('Accessing virtual component module')
            locals()['__name__'] = self._path  # for setup.load() validator
            getattr(importlib.import_module(entrypoint.__module__), entrypoint.__name__)(component)

        if not package:
            package = secrets.token_urlsafe(16)
        self._path = f'{virtual.__name__}.{package}'
        LOGGER.debug('Registering virtual component [%s]: %s', component, self._path)
        sys.meta_path[:0] = setupmod.Finder.create(types.ModuleType(self._path), onexec)

    @property
    def path(self) -> str:
        """The virtual path representing this component.

        Returns:
            Virtual component module path.
        """
        return self._path
