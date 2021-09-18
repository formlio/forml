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
import collections
import typing

from forml.flow.graph import node, port
from forml.flow.pipeline import topology


class Outcome(typing.NamedTuple):
    """Ytrue/Ypred outcome pair of DAG Paths."""

    true: port.Publishable
    """True outcome port."""
    pred: port.Publishable
    """Predicted outcome port."""


class Source(typing.NamedTuple):
    features: port.Subscriptable
    """Features input port."""
    labels: port.Subscriptable
    """Labels input port."""

    def outcomes(self, *outcomes: Outcome) -> 'Set':
        return Set(self, *outcomes)


class Score(typing.NamedTuple):
    """Evaluation dataset is a DAG producing Ytrue/Ypred data suitable for metric evaluation."""

    source: Source
    value: port.Publishable


class Set(collections.namedtuple('Set', 'source, outcomes')):
    """Evaluation dataset is a DAG producing Ytrue/Ypred data suitable for metric evaluation."""

    source: Source
    outcomes: typing.Tuple[Outcome]

    def __new__(cls, source: Source, *outcomes: Outcome):
        return super().__new__(cls, source, outcomes)

    def score(self, value: port.Publishable) -> Score:
        return Score(self.source, value)


class Metric(abc.ABC):
    """Evaluation metric interface."""

    @abc.abstractmethod
    def compose(self, *outcomes: Outcome) -> port.Publishable:
        """Compose the metric evaluation on top of the dataset DAG and return the tail node of the new DAG that's
        expected to have single output apply port delivering the calculated metric.

        Args:
            outcomes: Individual outcomes to be scored.

        Returns:
            Single node with one apply output providing the metric output.
        """

    def score(self, data: Set) -> Score:
        """Frontend method for returning the complete DAG implementing the metric evaluation."""
        return data.score(self.compose(*data.outcomes))


class Method(abc.ABC):
    """Interface for extending the pipeline DAG with the logic producing a dataset of Ytrue and Ypred columns that can
    be passed to a Metric for evaluation.

    Implementations of this interface can deliver the different evaluation techniques using strategies like holdout
    or cross-validation etc.
    """

    @abc.abstractmethod
    def apply(self, pipeline: topology.Composable) -> Set:
        """Compose the evaluation DataSet DAG producing the ytrue/ypred values.

        The operator is only expected to be engaged using its train/label paths.
        """
