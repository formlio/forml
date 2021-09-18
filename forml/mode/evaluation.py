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

from forml.flow import pipeline as pipemod, error
from forml.flow.graph import node, port
from forml.flow.pipeline import topology


class Outcome(typing.NamedTuple):
    """Ytrue/Ypred outcome pair of output ports."""

    true: port.Publishable
    """True outcome port."""
    pred: port.Publishable
    """Predicted outcome port."""


class Source(typing.NamedTuple):
    """Evaluation pipeline source ports."""

    features: port.Subscriptable
    """Features input port."""
    labels: port.Subscriptable
    """Labels input port."""

    def produce(self, *outcomes: Outcome) -> 'Product':
        """Utility method for creating the evaluation Product using the provided outcomes.

        Args:
            outcomes: Individual outcomes.
        """
        return Product(self, *outcomes)


class Score(topology.Operator):
    """Evaluation result value operator."""

    def __init__(self, source: Source, value: node.Atomic):
        if value.szout > 1:
            raise error.Topology('Simple output required')
        self._source: Source = source
        self._value: node.Atomic = value

    def compose(self, left: topology.Composable) -> pipemod.Segment:
        source = left.expand()
        self._source.features.subscribe(source.train.publisher)
        self._source.labels.subscribe(source.label.publisher)
        return source.use(source.apply.extend(tail=self._value))


class Product(collections.namedtuple('Product', 'source, outcomes')):
    """Evaluation dataset is a DAG producing Ytrue/Ypred data suitable for metric evaluation."""

    source: Source
    outcomes: tuple[Outcome]

    def __new__(cls, source: Source, *outcomes: Outcome):
        return super().__new__(cls, source, outcomes)

    def score(self, value: node.Atomic) -> Score:
        """Helper method for creating the Score operator.

        Args:
            value: Node (already connected to the DAG flowing from source) implementing the scoring.

        Returns:
            Operator that can be composed with feed/sink to produce the evaluation score.
        """
        return Score(self.source, value)


class Metric(abc.ABC):
    """Evaluation metric interface."""

    @abc.abstractmethod
    def score(self, *outcomes: Outcome) -> node.Atomic:
        """Compose the metric evaluation on top of the dataset DAG and return the tail node of the new DAG that's
        expected to have single output apply port delivering the calculated metric.

        Args:
            outcomes: Individual outcomes to be scored.

        Returns:
            Single node with one apply output providing the metric output.
        """

    def __repr__(self):
        return f'{self.__class__.__name__}Metric'

    def __call__(self, data: Product) -> Score:
        """Frontend method for returning the complete Score operator implementing the metric evaluation."""
        return data.score(self.score(*data.outcomes))


class Method(abc.ABC):
    """Interface for extending the pipeline DAG with the logic producing a dataset of Ytrue and Ypred columns that can
    be passed to a Metric for evaluation.

    Implementations of this interface can deliver the different evaluation techniques using strategies like holdout
    or cross-validation etc.
    """

    @abc.abstractmethod
    def produce(
        self, features: port.Publishable, label: port.Publishable, pipeline: topology.Composable
    ) -> typing.Iterable[Outcome]:
        """Compose the DAGs producing the ytrue/ypred outcomes.

        Args:
            features: Source port producing the input features.
            label: Source port producing the input labels.
            pipeline: Evaluation processing subject.

        Returns:
            Set of Ytrue/Ypred outcome pairs.
        """

    def __repr__(self):
        return f'{self.__class__.__name__}Method'

    def __call__(self, pipeline: topology.Composable) -> Product:
        """Frontend method for providing the evaluation product DAG.

        Args:
            pipeline: Evaluation processing subject.

        Returns:
            Evaluation product (suitable for use with a Metric implementation).
        """
        features = node.Future()
        label = node.Future()
        source = Source(features[0].subscriber, label[0].subscriber)
        return source.produce(*self.produce(features[0].publisher, label[0].publisher, pipeline))
