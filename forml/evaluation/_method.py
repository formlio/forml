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

from forml import flow
from forml.pipeline import payload

from . import _api

if typing.TYPE_CHECKING:
    from forml import evaluation


class CrossVal(_api.Method):
    """Cross validation ytrue/ypred dataset producer."""

    @typing.overload
    def __init__(
        self,
        *,
        crossvalidator: payload.CrossValidable,
        splitter: type[payload.CVFoldable] = payload.PandasCVFolds,
        nsplits: None = None,
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and a folding actor type.

        Parameter nsplits must not be provided.

        Args:
            crossvalidator: Implementation of the split selection logic.
            splitter: Folding actor type that is expected to take crossvalidator is its parameter.
                      Defaults to `payload.PandasCDFolds`.
        """

    @typing.overload
    def __init__(self, *, crossvalidator: None = None, splitter: flow.Spec[payload.CVFoldable], nsplits: int):
        """CrossVal constructor based on splitter supplied in form of a Spec object.

        Crossvalidator must not be provided.

        Args:
            splitter: Spec object defining the folding splitter.
            nsplits: Number of splits the splitter is going to generate (needs to be explicit as there is no reliable
                     way to extract it from the Spec).
        """

    def __init__(self, *, crossvalidator=None, splitter=payload.PandasCVFolds, nsplits=None):
        if ((crossvalidator is None) ^ (nsplits is not None)) or (
            (crossvalidator is None) ^ isinstance(splitter, flow.Spec)
        ):
            raise TypeError('Invalid combination of crossvalidator, splitter and nsplits')
        if not isinstance(splitter, flow.Spec):
            splitter = splitter.spec(crossvalidator=crossvalidator)
            nsplits = crossvalidator.get_n_splits()
        if nsplits < 2:
            raise ValueError('At least 2 splits required')
        self._nsplits: int = nsplits
        self._splitter: flow.Spec[payload.CVFoldable] = splitter

    def produce(
        self, pipeline: flow.Composable, features: flow.Publishable, label: flow.Publishable
    ) -> typing.Iterable['evaluation.Outcome']:
        splitter = flow.Worker(self._splitter, 1, 2 * self._nsplits)
        splitter.train(features, label)

        features_splits: flow.Worker = splitter.fork()
        features_splits[0].subscribe(features)
        label_splits: flow.Worker = splitter.fork()
        label_splits[0].subscribe(label)

        outcomes = []
        for fid in range(self._nsplits):
            fold: flow.Trunk = pipeline.expand()
            fold.train.subscribe(features_splits[2 * fid])
            fold.label.subscribe(label_splits[2 * fid])
            fold.apply.subscribe(features_splits[2 * fid + 1])
            outcomes.append(_api.Outcome(label_splits[2 * fid + 1].publisher, fold.apply.publisher))
        return tuple(outcomes)


# class HoldOut(_api.Method):
#     def __init__(self, test_size: ):
