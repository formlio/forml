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
Metric implementations.
"""
import statistics
import typing

from forml.flow import task
from forml.flow.graph import node
from forml.lib.flow import topology
from forml.mode import evaluation


class Function(evaluation.Metric):
    """Basic metric wrapping a plain scoring function."""

    def __init__(
        self,
        metric: typing.Callable[[typing.Any, typing.Any], float],
        reducer: typing.Callable[..., float] = lambda *m: statistics.mean(m),
    ):
        self._metric: task.Spec = topology.Function.Actor.spec(function=metric)
        self._reducer: task.Spec = topology.Function.Actor.spec(function=reducer)

    def score(self, *outcomes: evaluation.Outcome) -> node.Worker:
        def apply(fold: evaluation.Outcome) -> node.Worker:
            """Score the given outcome fold.

            Args:
                fold: Outcome to be scored.

            Returns:
                Worker node implementing the scoring for this fold.
            """
            worker = node.Worker(self._metric, 2, 1)
            worker[0].subscribe(fold.true)
            worker[1].subscribe(fold.pred)
            return worker

        def merge(reducer: node.Worker, fold: node.Worker, index: int) -> node.Worker:
            """Merge the given fold using the provided reducer under the given fold index.

            Args:
                reducer: Reducer worker node.
                fold: Fold worker node.
                index: Fold index.

            Returns:
                Reducer worker node.
            """
            reducer[index].subscribe(fold[0])
            return reducer

        assert outcomes, 'Expecting outcomes.'
        result = apply(outcomes[0])
        if (fold_count := len(outcomes)) > 1:
            result = merge(node.Worker(self._reducer, fold_count, 1), result, 0)
            for idx, out in enumerate(outcomes[1:], start=1):
                merge(result, apply(out), idx)
        return result
