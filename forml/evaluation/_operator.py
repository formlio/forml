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
import typing

from forml import flow

from . import _api

if typing.TYPE_CHECKING:
    from forml import evaluation


class TrainScore(flow.Operator):
    """Development evaluation result value operator.

    This assumes no pre-existing state - pipeline is trained in scope of the evaluation.
    Only the train path of the composed trunk is expected to be used.
    """

    def __init__(self, metric: 'evaluation.Metric', method: 'evaluation.Method'):
        self._metric: 'evaluation.Metric' = metric
        self._method: 'evaluation.Method' = method

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

    def __init__(self, metric: 'evaluation.Metric'):
        self._metric: 'evaluation.Metric' = metric

    def compose(self, left: flow.Composable) -> flow.Trunk:
        head: flow.Trunk = flow.Trunk()
        pipeline: flow.Trunk = left.expand()
        pipeline.apply.copy().subscribe(head.apply)  # all persistent nodes must be reachable via the apply path
        pipeline.apply.subscribe(head.train)
        value = self._metric.score(_api.Outcome(head.label.publisher, pipeline.apply.publisher))
        return head.use(train=head.train.extend(tail=value))
