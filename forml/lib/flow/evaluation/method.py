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
from sklearn import model_selection

from forml.flow import task, pipeline
from forml.flow.graph import node
from forml.flow.pipeline import topology
from forml.lib.flow.actor import ndframe
from forml.mode import evaluation


class CrossVal(evaluation.Method):
    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self._splitter: task.Spec = ndframe.TrainTestSplit.spec(crossvalidator=crossvalidator)

    @property
    def nsplits(self) -> int:
        """Get the number of folds.

        Returns:
            Number of folds.
        """
        return self._splitter.kwargs['crossvalidator'].get_n_splits()

    def apply(self, left: topology.Composable) -> evaluation.Set:
        head: pipeline.Segment = pipeline.Segment()
        splitter = node.Worker(self._splitter, 1, 2 * self.nsplits)
        splitter.train(head.train.publisher, head.label.publisher)
        features: node.Worker = splitter.fork()
        features[0].subscribe(head.train.publisher)
        labels: node.Worker = splitter.fork()
        labels[0].subscribe(head.label.publisher)
        source = evaluation.Source(head.train.subscriber, head.label.subscriber)

        outcomes = list()
        for idx in range(self.nsplits):
            fold: pipeline.Segment = left.expand()
            fold.train.subscribe(features[2 * idx])
            fold.label.subscribe(labels[2 * idx])
            fold.apply.subscribe(features[2 * idx + 1])
            outcomes.append(evaluation.Outcome(labels[2 * idx + 1].publisher, fold.apply.publisher))
        return source.outcomes(*outcomes)


# class HoldOut(evaluation.Method):
#     def __init__(self, test_size: ):
