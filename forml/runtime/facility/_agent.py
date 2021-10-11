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
import logging
import typing

import forml
from forml import _provider as provmod
from forml import flow, io
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.runtime import asset, code
from forml.runtime.mode import evaluation

LOGGER = logging.getLogger(__name__)


class Runner(provmod.Interface, default=provcfg.Runner.default, path=provcfg.Runner.path):
    """Abstract base runner class to be extended by particular runner implementations."""

    def __init__(
        self,
        instance: typing.Optional[asset.Instance] = None,
        feed: typing.Optional[io.Feed] = None,
        sink: typing.Optional[io.Sink] = None,
        **_,
    ):
        self._instance: asset.Instance = instance or asset.Instance()
        self._feed: io.Feed = feed or io.Feed()
        self._sink: typing.Optional[io.Sink] = sink

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
        composition = self._build(lower, upper, self._instance.project.pipeline)
        self._exec(composition.apply, self._instance.state(composition.persistent))

    def tune(  # pylint: disable=no-self-use
        self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None
    ) -> None:
        """Run the tune mode.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        raise forml.MissingError('Not yet supported')

    def train_eval(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        """Run the development mode evaluation (based on training model from scratch).

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        spec = self._instance.project.evaluation
        if not spec:
            raise forml.MissingError('Project not evaluable')

        composition = self._build(
            lower, upper, self._instance.project.pipeline >> evaluation.TrainScore(spec.metric, spec.method)
        )
        self._exec(composition.train)

    def apply_eval(self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None) -> None:
        """Run the production mode evaluation (predictions of already trained model).

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        spec = self._instance.project.evaluation
        if not spec:
            raise forml.MissingError('Project not evaluable')

        composition = self._build(lower, upper, self._instance.project.pipeline >> evaluation.ApplyScore(spec.metric))
        self._exec(composition.train, self._instance.state(composition.persistent))

    def _build(
        self, lower: typing.Optional[dsl.Native], upper: typing.Optional[dsl.Native], *blocks: flow.Composable
    ) -> flow.Composition:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble (each with its own composition domain).

        Returns:
            Assembled flow pipeline.
        """
        segments = [self._feed.load(self._instance.project.source, lower, upper)]
        segments.extend(b.expand() for b in blocks)
        if self._sink:
            segments.append(self._sink.publish())
        return flow.Composition(*segments)

    def _exec(self, path: flow.Path, assets: typing.Optional[asset.State] = None) -> None:
        """Execute the given path and assets.

        Args:
            path: Pipeline path.
            assets: Persistent assets to be used.

        Returns:
            Optional return value.
        """
        return self._run(code.generate(path, assets))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[code.Symbol]) -> None:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.

        Returns:
            Optional pipeline return value.
        """
