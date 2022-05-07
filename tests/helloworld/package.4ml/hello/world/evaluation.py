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
Dummy project evaluation.
"""
import typing

from sklearn import metrics, model_selection

from forml import evaluation, flow, project
from forml.pipeline import payload


class Splitter(payload.CVFoldable[typing.Sequence[tuple[str, str, int]], typing.Sequence[int], None]):
    """Tuple based splitter implementation."""

    @classmethod
    def split(
        cls,
        features: typing.Sequence[tuple[str, str, int]],
        indices: typing.Sequence[tuple[typing.Sequence[int], typing.Sequence[int]]],
    ) -> typing.Sequence[flow.Features]:
        return tuple(s for a, b in indices for s in ([features[i] for i in a], [features[i] for i in b]))


INSTANCE = project.Evaluation(
    evaluation.Function(metrics.mean_squared_error),
    evaluation.CrossVal(
        crossvalidator=model_selection.KFold(n_splits=2, shuffle=True, random_state=42), splitter=Splitter
    ),
)
project.setup(INSTANCE)
