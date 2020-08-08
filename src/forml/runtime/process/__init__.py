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
Runtime process layer.
"""
import abc
import typing

from forml import provider, error, io
from forml.conf import provider as provcfg
from forml.flow import pipeline
from forml.io.dsl.schema import kind
from forml.runtime import code
from forml.runtime.asset import access
from forml.runtime.code import compiler


class Runner(provider.Interface, default=provcfg.Runner.default):
    """Abstract base runtime class to be extended by particular runtime implementations.
    """
    def __init__(self, assets: typing.Optional[access.Assets] = None, feed: typing.Optional['io.Feed'] = None):
        self._assets: access.Assets = assets or access.Assets()
        self._feed: io.Feed = feed or io.Feed()

    def train(self, lower: typing.Optional['kind.Native'] = None,
              upper: typing.Optional['kind.Native'] = None) -> typing.Any:
        """Run the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._build(lower or self._assets.tag.training.ordinal, upper,
                                  self._assets.project.pipeline)
        return self._exec(composition.train,
                          self._assets.state(composition.shared, self._assets.tag.training.trigger()))

    def apply(self, lower: typing.Optional['kind.Native'] = None,
              upper: typing.Optional['kind.Native'] = None) -> typing.Any:
        """Run the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        composition = self._build(lower, upper, self._assets.project.pipeline)
        return self._exec(composition.apply, self._assets.state(composition.shared))

    def cvscore(self, lower: typing.Optional['kind.Native'] = None,
                upper: typing.Optional['kind.Native'] = None) -> typing.Any:
        """Run the crossvalidating evaluation.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Crossvalidate evaluation score.
        """
        return self._exec(self._evaluation(lower, upper).train)

    def _evaluation(self, lower: typing.Optional['kind.Native'] = None,
                    upper: typing.Optional['kind.Native'] = None) -> pipeline.Segment:
        """Return the evaluation pipeline.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Evaluation pipeline.
        """
        if not self._assets.project.evaluation:
            raise error.Invalid('Project not evaluable')
        return self._build(lower, upper, self._assets.project.pipeline >> self._assets.project.evaluation)

    def _build(self, lower: typing.Optional['kind.Native'], upper: typing.Optional['kind.Native'],
               *blocks: pipeline.Segment) -> pipeline.Composition:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns: Assembled flow pipeline.
        """
        return pipeline.Composition(self._feed.load(self._assets.project.source, lower, upper),
                                    *(b.expand() for b in blocks))

    def _exec(self, path: pipeline.Segment, assets: typing.Optional[access.State] = None) -> typing.Any:
        """Execute the given path and assets.

        Args:
            path: Pipeline path.
            assets: Persistent assets to be used.

        Returns: Optional return value.
        """
        return self._run(compiler.generate(path, assets))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[code.Symbol]) -> typing.Any:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.

        Returns: Optional pipeline return value.
        """
