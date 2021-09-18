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
Data splitting functionality.
"""
import typing

from sklearn import model_selection

from forml.flow import task, pipeline as pipemod
from forml.flow.graph import node, port
from forml.flow.pipeline import topology
from forml.lib.flow.actor import ndframe
from forml.mode import evaluation


class CrossVal(evaluation.Method):
    """Cross validation ytrue/ypred dataset producer."""

    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self._splitter: task.Spec = ndframe.TrainTestSplit.spec(crossvalidator=crossvalidator)

    @property
    def nsplits(self) -> int:
        """Get the number of folds.

        Returns:
            Number of folds.
        """
        return self._splitter.kwargs['crossvalidator'].get_n_splits()

    def produce(
        self, features: port.Publishable, label: port.Publishable, pipeline: topology.Composable
    ) -> typing.Iterable[evaluation.Outcome]:
        splitter = node.Worker(self._splitter, 1, 2 * self.nsplits)
        splitter.train(features, label)
        features: node.Worker = splitter.fork()
        features[0].subscribe(features)
        labels: node.Worker = splitter.fork()
        labels[0].subscribe(label)

        outcomes = []
        for idx in range(self.nsplits):
            fold: pipemod.Segment = pipeline.expand()
            fold.train.subscribe(features[2 * idx])
            fold.label.subscribe(labels[2 * idx])
            fold.apply.subscribe(features[2 * idx + 1])
            outcomes.append(evaluation.Outcome(labels[2 * idx + 1].publisher, fold.apply.publisher))
        return tuple(outcomes)


# class HoldOut(evaluation.Method):
#     def __init__(self, test_size: ):
