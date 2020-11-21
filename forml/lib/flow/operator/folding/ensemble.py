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
Ensembling operators.
"""

import typing

import pandas
from pandas.core import generic as pdtype
from sklearn import model_selection

from forml.flow import pipeline
from forml.flow.graph import node
from forml.flow.pipeline import topology
from forml.lib.flow.actor import ndframe
from forml.lib.flow.operator import folding


class FullStacker(folding.Crossvalidated):
    """Stacked ensembling with all individual models kept for serving."""

    class Builder(folding.Crossvalidated.Builder):
        """Crossvalidation builder used as a folding context."""

        def __init__(
            self,
            head: pipeline.Segment,
            stackers: typing.Dict[topology.Composable, node.Worker],
            mergers: typing.Dict[topology.Composable, node.Worker],
            trained: node.Worker,
            applied: node.Worker,
        ):
            self.head: pipeline.Segment = head
            self.stackers: typing.Dict[topology.Composable, node.Worker] = stackers
            self.mergers: typing.Dict[topology.Composable, node.Worker] = mergers
            self.trained: node.Worker = trained
            self.applied: node.Worker = applied

        def build(self) -> pipeline.Segment:
            """Builder finalize method.

            Returns:
                Crossvalidation pipeline segment.
            """
            return self.head.use(
                apply=self.head.apply.extend(tail=self.applied), train=self.head.train.extend(tail=self.trained)
            )

    def __init__(self, bases: typing.Sequence[topology.Composable], crossvalidator: model_selection.BaseCrossValidator):
        super().__init__(crossvalidator)
        self.bases: typing.Sequence[topology.Composable] = bases

    @staticmethod
    def _merge(*folds: pdtype.NDFrame) -> pandas.DataFrame:
        """Individual fold model predictions merging.

        Predictions can either be series or multicolumn dataframes in which case same position columns are merged
        together.

        Args:
            *folds: Individual model predictions.

        Returns:
            Single dataframe with the merged predictions.
        """
        if not (folds and all(f.shape == folds[0].shape for f in folds)):
            raise ValueError('Folds must have same shape')
        folds = (
            [([f.name for f in folds], folds)]
            if folds[0].ndim == 1
            else (zip(*i) for i in zip(*(f.iteritems() for f in folds)))
        )
        return pandas.concat(
            (pandas.concat(s, axis='columns').mean(axis='columns').rename(n[0]) for n, s in folds), axis='columns'
        )

    def builder(self, head: pipeline.Segment, inner: pipeline.Segment) -> 'FullStacker.Builder':
        """Create a builder (folding context).

        Args:
            head: Head of the crossvalidation segment.
            inner: Exclusive instance of the inner composition.

        Returns:
            Builder instance.
        """
        trained: node.Worker = node.Worker(ndframe.Concat.spec(axis='columns'), len(self.bases), 1)
        applied: node.Worker = trained.fork()
        stack_forks: typing.Iterable[node.Worker] = node.Worker.fgen(ndframe.Concat.spec(axis='index'), self.nsplits, 1)
        merge_forks: typing.Iterable[node.Worker] = node.Worker.fgen(
            ndframe.Apply.spec(function=self._merge), self.nsplits, 1
        )
        stackers: typing.Dict[topology.Composable, node.Worker] = dict()
        mergers: typing.Dict[topology.Composable, node.Worker] = dict()
        for index, (base, stack, merge) in enumerate(zip(self.bases, stack_forks, merge_forks)):
            stackers[base] = stack
            mergers[base] = merge
            trained[index].subscribe(stackers[base][0])
            applied[index].subscribe(merge[0])

        return self.Builder(head, stackers, mergers, trained, applied)

    def fold(
        self,
        fold: int,
        builder: 'FullStacker.Builder',  # pylint: disable=arguments-differ
        pretrack: pipeline.Segment,
        features: node.Worker,
        labels: node.Worker,
    ) -> None:
        """Implement single fold ensembling.

        Args:
            fold: Fold index.
            builder: Composition builder (folding context).
            pretrack: Exclusive instance of the inner composition.
            features: Features splitter actor.
            labels: Labels splitter actor.
        """
        pretrack.train.subscribe(features[2 * fold])
        pretrack.label.subscribe(labels[2 * fold])
        pretrack.apply.subscribe(builder.head.apply.publisher)
        preapply = pretrack.apply.copy()
        preapply.subscribe(features[2 * fold + 1])
        for base in self.bases:
            basetrack = base.expand()
            basetrack.train.subscribe(pretrack.train.publisher)
            basetrack.label.subscribe(pretrack.label.publisher)
            basetrack.apply.subscribe(pretrack.apply.publisher)
            builder.mergers[base][fold].subscribe(basetrack.apply.publisher)
            baseapply = basetrack.apply.copy()
            baseapply.subscribe(preapply.publisher)
            builder.stackers[base][fold].subscribe(baseapply.publisher)
