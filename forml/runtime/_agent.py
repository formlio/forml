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
Executor component.
"""
import abc
import functools
import logging
import typing

import forml
from forml import evaluation
from forml import flow as flowmod
from forml import io as iomod
from forml import project, provider, setup
from forml.io import asset as assetmod
from forml.io import dsl

if typing.TYPE_CHECKING:
    from forml import flow, io  # pylint: disable=reimported
    from forml.io import asset  # pylint: disable=reimported

LOGGER = logging.getLogger(__name__)


class Runner(provider.Service, default=setup.Runner.default, path=setup.Runner.path):
    """Base class for implementing ForML runner providers.

    The public API allows performing all the standard actions of the :doc:`ForML lifecycles
    <lifecycle>`.

    All that needs to be supplied by the provider is the abstract :meth:`run` method.

    Args:
        instance: A particular instance of the persistent artifacts to be executed.
        feed: Optional input feed instance to retrieve the data from (falls back to the default
              configured feed).
        sink: Output sink instance (no output is produced if omitted).
        kwargs: Additional keyword arguments for the :meth:`run` method.
    """

    _METRIC_SCHEMA = dsl.Schema.from_fields(dsl.Field(dsl.Float(), name='Metric'))

    def __init__(
        self,
        instance: typing.Optional['asset.Instance'] = None,
        feed: typing.Optional['io.Feed'] = None,
        sink: typing.Optional['io.Sink'] = None,
        **kwargs,
    ):
        self._instance: 'asset.Instance' = instance or assetmod.Instance()
        self._feed: 'io.Feed' = feed or iomod.Feed()
        self._sink: typing.Optional['io.Sink'] = sink
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def start(self) -> None:
        """Runner startup routine."""

    def close(self) -> None:
        """Runner shutdown routine."""

    def __enter__(self) -> 'Runner':
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def train(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        """Run the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._build(lower or self._instance.tag.training.ordinal, upper, self._instance.project.pipeline)
        self._exec(
            composition.train, self._instance.state(composition.persistent, self._instance.tag.training.trigger())
        )

    def apply(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        """Run the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._build(lower, upper, self._instance.project.pipeline, output=None)  # TO-DO: sink schema
        self._exec(composition.apply, self._instance.state(composition.persistent))

    def tune(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        """Run the tune mode.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        raise forml.MissingError('Not yet supported')

    def eval_traintest(
        self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None
    ) -> None:
        """Run the development mode (backtesting) evaluation (training the model from scratch).

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._eval(lower, upper, lambda s: evaluation.TrainTestScore(s.metric, s.method))
        self._exec(composition.train)

    def eval_perftrack(
        self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None
    ) -> None:
        """Run the production performance tracking evaluation (predictions of an existing model).

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._eval(lower, upper, lambda s: evaluation.PerfTrackScore(s.metric))
        self._exec(composition.train, self._instance.state(composition.persistent))

    def _eval(
        self,
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
        evaluator: typing.Callable[[project.Evaluation], 'flow.Operator'],
    ) -> 'flow.Composition':
        """Helper for setting up the evaluation composition.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
            evaluator: Callback to provide an operator based on the given evaluation spec.

        Returns:
            Evaluation composition.
        """
        spec = self._instance.project.evaluation
        if not spec:
            raise forml.MissingError('Project not evaluable')

        return self._build(
            lower,
            upper,
            self._instance.project.pipeline >> evaluator(spec),
            output=self._METRIC_SCHEMA,
        )

    def _build(
        self,
        lower: typing.Optional[dsl.Native],
        upper: typing.Optional[dsl.Native],
        *blocks: 'flow.Composable',
        output: typing.Optional[dsl.Source.Schema] = None,
    ) -> 'flow.Composition':
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            blocks: Composable block to assemble (each with its own composition domain).
            output: Output schema.

        Returns:
            Assembled flow pipeline.
        """
        composition = flowmod.Composition.builder(
            self._feed.load(self._instance.project.source.extract, lower, upper),
            self._instance.project.source.transform,
        )
        composition = functools.reduce(flowmod.Composition.Builder.via, blocks, composition)
        return composition.build(self._sink.save(output) if self._sink else None)

    def _exec(self, segment: 'flow.Segment', assets: typing.Optional['asset.State'] = None) -> None:
        """Execute the given segment and assets.

        Args:
            segment: Pipeline segment.
            assets: Persistent assets to be used.

        Returns:
            Optional return value.
        """
        return self.run(flowmod.compile(segment, assets), **self._kwargs)

    @classmethod
    @abc.abstractmethod
    def run(cls, symbols: typing.Collection['flow.Symbol'], **kwargs) -> None:
        """Actual run action implementation using the specific provider execution technology.

        Args:
            symbols: Collection of portable symbols representing the workflow task graph to be
                     executed as produced by the :func:`flow.compile() <forml.flow.compile>`
                     function.
            kwargs: Custom keyword arguments provided via the constructor.
        """
        raise NotImplementedError()
