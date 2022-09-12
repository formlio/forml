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
Evaluation mode functionality.
"""
import abc
import typing

if typing.TYPE_CHECKING:
    from forml import evaluation, flow


class Outcome(typing.NamedTuple):
    """*True* and *predicted* outcome pair of output ports.

    These two ports provide all the data required to calculate the evaluation metric.
    """

    true: 'flow.Publishable'
    """True outcomes port."""
    pred: 'flow.Publishable'
    """Predicted outcomes port."""


class Metric(abc.ABC):
    """Evaluation metric interface."""

    @abc.abstractmethod
    def score(self, *outcomes: 'evaluation.Outcome') -> 'flow.Node':
        """Compose the metric evaluation task on top of the given *outcomes* DAG ports.

        Return the tail node of the new DAG that's expected to have a single output apply-port
        delivering the calculated metric.

        Args:
            outcomes: Individual outcomes partitions to be scored.

                      Note:
                        The input is (potentially) a *sequence* of outcome partitions as the
                        metrics might need to be calculated from separate chunks (e.g. individual
                        cross-validation folds).

        Returns:
            Single node with single output apply-port providing the metric output.
        """

    def __repr__(self):
        return f'{self.__class__.__name__}Metric'


class Method(abc.ABC):
    """Interface for extending the pipeline DAG with the logic for producing *true* and *predicted*
    outcome columns from historical data.

    Attention:
        The *method* is only producing the *true*/*prediction* outcome pairs - not an evaluation
        result. The outcomes are expected to be passed to some :class:`evaluation.Metric
        <forml.evaluation.Metric>` implementation for the actual scoring.

    Implementations of this interface can deliver different evaluation techniques using strategies
    like *holdout* or *cross-validation* etc.
    """

    @abc.abstractmethod
    def produce(
        self, pipeline: 'flow.Composable', features: 'flow.Publishable', labels: 'flow.Publishable'
    ) -> typing.Iterable['evaluation.Outcome']:
        """Compose the DAG producing the true/predicted outcomes according to the given method.

        Args:
            pipeline: Evaluation subject - the solution pipeline to be backtested.
            features: Source port producing the historical features.
            labels: Source port producing the historical outcomes matching the features.

        Returns:
            A sequence of true/predicted outcome port pairs.
        """

    def __repr__(self):
        return f'{self.__class__.__name__}Method'
