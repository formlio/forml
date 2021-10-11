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

from forml import flow


class Outcome(typing.NamedTuple):
    """Ytrue/Ypred outcome pair of output ports."""

    true: flow.Publishable
    """True outcome port."""
    pred: flow.Publishable
    """Predicted outcome port."""


class Metric(abc.ABC):
    """Evaluation metric interface."""

    @abc.abstractmethod
    def score(self, *outcomes: Outcome) -> flow.Atomic:
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
        self, pipeline: flow.Composable, features: flow.Publishable, label: flow.Publishable
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


class TrainScore(flow.Operator):
    """Development evaluation result value operator.

    This assumes no pre-existing state - pipeline is trained in scope of the evaluation.
    Only the train path of the composed trunk is expected to be used.
    """

    def __init__(self, metric: Metric, method: Method):
        self._metric: Metric = metric
        self._method: Method = method

    def compose(self, left: flow.Composable) -> flow.Trunk:
        head: flow.Trunk = flow.Trunk()
        outcomes = self._method.produce(left, head.train.publisher, head.label.publisher)
        value = self._metric.score(*outcomes)
        return head.use(train=head.train.extend(tail=value))


class ApplyScore(flow.Operator):
    """Production evaluation result value operator.

    This assumes pre-existing state of the pipeline trained previously.

    Only the train path of the composed trunk is expected to be used (apply path still needs to present all persistent
    nodes so that the states can be loaded).
    """

    def __init__(self, metric: Metric):
        self._metric: Metric = metric

    def compose(self, left: flow.Composable) -> flow.Trunk:
        head: flow.Trunk = flow.Trunk()
        pipeline: flow.Trunk = left.expand()
        pipeline.apply.copy().subscribe(head.apply)  # all persistent nodes must be reachable via the apply path
        pipeline.apply.subscribe(head.train)
        value = self._metric.score(Outcome(head.label.publisher, pipeline.apply.publisher))
        return head.use(train=head.train.extend(tail=value))
