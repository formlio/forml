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

from forml.flow import pipeline as pipemod
from forml.flow.graph import node, port
from forml.flow.pipeline import topology


class Outcome(typing.NamedTuple):
    """Ytrue/Ypred outcome pair of output ports."""

    true: port.Publishable
    """True outcome port."""
    pred: port.Publishable
    """Predicted outcome port."""


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


class Method(abc.ABC):
    """Interface for extending the pipeline DAG with the logic producing a dataset of Ytrue and Ypred columns that can
    be passed to a Metric for evaluation.

    Implementations of this interface can deliver the different evaluation techniques using strategies like holdout
    or cross-validation etc.
    """

    @abc.abstractmethod
    def produce(
        self, pipeline: topology.Composable, features: port.Publishable, label: port.Publishable
    ) -> typing.Iterable[Outcome]:
        """Compose the DAGs producing the ytrue/ypred outcomes.

        Args:
            pipeline: Evaluation processing subject.
            features: Source port producing the input features.
            label: Source port producing the input labels.

        Returns:
            Set of Ytrue/Ypred outcome pairs.
        """

    def __repr__(self):
        return f'{self.__class__.__name__}Method'


class Score(topology.Operator):
    """Evaluation result value operator."""

    def __init__(self, method: Method, metric: Metric):
        self._method: Method = method
        self._metric: Metric = metric

    def compose(self, left: topology.Composable) -> pipemod.Segment:
        head: pipemod.Segment = pipemod.Segment()
        outcomes = self._method.produce(left, head.train.publisher, head.label.publisher)
        value = self._metric.score(*outcomes)
        return head.use(train=head.train.extend(tail=value))
