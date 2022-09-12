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

from forml import flow
from forml.pipeline import payload

from . import _api

if typing.TYPE_CHECKING:
    from forml import evaluation


class Function(_api.Metric):
    """Function(metric: Callable[[typing.Any, typing.Any], float], reducer: Callable[..., float] = mean)

    Basic metric implementation wrapping a plain scoring function.

    Caution:
        As with any ForML task, the implementer is responsible for engaging a function that is
        compatible with the particular :ref:`payload <io-payload>`.

    Args:
        metric: Actual metric function implementation.
        reducer: Callable to reduce individual metric *partitions* into a single final value.
                 It must accept as many positional arguments as many outcome partitions there are.
                 The default reducer is the :func:`python:statistics.mean`.

    Examples:
        >>> LOG_LOSS = evaluation.Function(sklearn.metrics.log_loss)
        >>> ACCURACY = evaluation.Function(
        ...     lambda t, p: sklearn.metrics.accuracy_score(t, numpy.round(p))
        ... )
    """

    def __init__(
        self,
        metric: typing.Callable[[typing.Any, typing.Any], float],
        reducer: typing.Callable[..., float] = lambda *m: statistics.mean(m),  # noqa: B008
    ):
        self._metric: flow.Builder = payload.Apply.builder(function=metric)
        self._reducer: flow.Builder = payload.Apply.builder(function=reducer)

    def score(self, *outcomes: 'evaluation.Outcome') -> flow.Worker:
        def apply(partition: 'evaluation.Outcome') -> flow.Worker:
            """Score the given outcome partition.

            Args:
                partition: Outcome to be scored.

            Returns:
                Worker node implementing the scoring for this partition.
            """
            worker = flow.Worker(self._metric, 2, 1)
            worker[0].subscribe(partition.true)
            worker[1].subscribe(partition.pred)
            return worker

        def merge(reducer: flow.Worker, partition: flow.Worker, index: int) -> flow.Worker:
            """Merge the given partition using the provided reducer under the given partition index.

            Args:
                reducer: Reducer worker flow.
                partition: Partition worker flow.
                index: Partition index.

            Returns:
                Reducer worker flow.
            """
            reducer[index].subscribe(partition[0])
            return reducer

        assert outcomes, 'Expecting outcomes.'
        result = apply(outcomes[0])
        if (partition_count := len(outcomes)) > 1:
            result = merge(flow.Worker(self._reducer, partition_count, 1), result, 0)
            for idx, out in enumerate(outcomes[1:], start=1):
                merge(result, apply(out), idx)
        return result
