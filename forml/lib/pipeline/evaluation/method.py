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

from forml import flow
from forml.lib.pipeline import payload
from forml.runtime.mode import evaluation


class CrossVal(evaluation.Method):
    """Cross validation ytrue/ypred dataset producer."""

    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self._splitter: flow.Spec = payload.CVFolds.spec(crossvalidator=crossvalidator)

    @property
    def nsplits(self) -> int:
        """Get the number of folds.

        Returns:
            Number of folds.
        """
        return self._splitter.kwargs['crossvalidator'].get_n_splits()

    def produce(
        self, pipeline: flow.Composable, features: flow.Publishable, label: flow.Publishable
    ) -> typing.Iterable[evaluation.Outcome]:
        splitter = flow.Worker(self._splitter, 1, 2 * self.nsplits)
        splitter.train(features, label)

        features_splits: flow.Worker = splitter.fork()
        features_splits[0].subscribe(features)
        label_splits: flow.Worker = splitter.fork()
        label_splits[0].subscribe(label)

        outcomes = []
        for fid in range(self.nsplits):
            fold: flow.Trunk = pipeline.expand()
            fold.train.subscribe(features_splits[2 * fid])
            fold.label.subscribe(label_splits[2 * fid])
            fold.apply.subscribe(features_splits[2 * fid + 1])
            outcomes.append(evaluation.Outcome(label_splits[2 * fid + 1].publisher, fold.apply.publisher))
        return tuple(outcomes)


# class HoldOut(evaluation.Method):
#     def __init__(self, test_size: ):
