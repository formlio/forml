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
Implementations of folding ensemblers.
"""

import abc
import typing

import pandas
from pandas.core import generic as pdtype
from sklearn import model_selection

from forml import flow
from forml.pipeline import payload


class Split(typing.NamedTuple):
    """Holder of data splits belonging to the same fold."""

    apply: flow.Publishable
    train: flow.Publishable
    label: flow.Publishable
    test: flow.Publishable

    def publish(self, apply: flow.Path, train: flow.Path, label: flow.Path, test: flow.Path):
        """Helper for connecting the individual data ports."""
        apply.subscribe(self.apply)
        train.subscribe(self.train)
        label.subscribe(self.label)
        test.subscribe(self.test)


class Ensembler(flow.Operator):
    """Base class for folding ensemblers."""

    class Stacker(abc.ABC):
        """Implementation of fold stacking."""

        def __init__(self, bases: typing.Iterable[flow.Composable]):
            self._bases: tuple[flow.Composable] = tuple(bases)

        @abc.abstractmethod
        def stack(self, folds: typing.Iterable[Split]) -> tuple[flow.Atomic, flow.Atomic]:
            """Stack the folds using the given bases and produce a tran and apply outputs.

            Args:
                folds: Iterable of the split data folds.

            Returns:
                Tuple of tail nodes returning the train and apply mode stack outputs.
            """

    def __init__(self, bases: typing.Sequence[flow.Composable], crossvalidator: model_selection.BaseCrossValidator):
        self._splitter: flow.Spec[
            pandas.DataFrame, pandas.Series, typing.Sequence[pandas.DataFrame]
        ] = payload.CVFolds.spec(crossvalidator=crossvalidator)
        self._stacker: Ensembler.Stacker = self.Stacker(bases)  # pylint: disable=abstract-class-instantiated

    @property
    def nsplits(self) -> int:
        """Get the number of folds.

        Returns:
            Number of folds.
        """
        return self._splitter.kwargs['crossvalidator'].get_n_splits()

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Ensemble composition.

        Args:
            left: left segment.

        Returns:
            Composed segment track.
        """
        head: flow.Trunk = flow.Trunk()
        input_splitter = flow.Worker(self._splitter, 1, 2 * self.nsplits)
        input_splitter.train(head.train.publisher, head.label.publisher)
        feature_folds: flow.Worker = input_splitter.fork()
        feature_folds[0].subscribe(head.train.publisher)
        label_folds: flow.Worker = input_splitter.fork()
        label_folds[0].subscribe(head.label.publisher)

        data_folds = []
        for fid in range(self.nsplits):
            pipeline_fold: flow.Trunk = left.expand()
            pipeline_fold.train.subscribe(feature_folds[2 * fid])
            pipeline_fold.label.subscribe(label_folds[2 * fid])
            pipeline_fold.apply.subscribe(head.apply.publisher)
            test_fold = pipeline_fold.apply.copy()
            test_fold.subscribe(feature_folds[2 * fid + 1])
            data_folds.append(
                Split(
                    pipeline_fold.apply.publisher,
                    pipeline_fold.train.publisher,
                    pipeline_fold.label.publisher,
                    test_fold.publisher,
                )
            )
        train_tail, apply_tail = self._stacker.stack(data_folds)
        return head.use(apply=head.apply.extend(tail=apply_tail), train=head.train.extend(tail=train_tail))


class FullStack(Ensembler):
    """Stacking ensembler with all individual models kept for serving."""

    class Stacker(Ensembler.Stacker):
        """Implementation of the fold stacking."""

        def stack(self, folds: typing.Iterable[Split]) -> tuple[flow.Atomic, flow.Atomic]:
            folds = tuple(folds)
            nsplits = len(folds)
            train_output: flow.Worker = flow.Worker(payload.Concat.spec(axis='columns'), len(self._bases), 1)
            apply_output: flow.Worker = train_output.fork()
            stacker_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(
                payload.Concat.spec(axis='index'), nsplits, 1
            )
            merger_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(
                payload.Apply.spec(function=self._merge), nsplits, 1
            )
            for base_idx, (base, stacker, merger) in enumerate(zip(self._bases, stacker_forks, merger_forks)):
                train_output[base_idx].subscribe(stacker[0])
                apply_output[base_idx].subscribe(merger[0])
                for fold_idx, pipeline_fold in enumerate(folds):
                    base_fold: flow.Trunk = base.expand()
                    fold_apply = base_fold.apply.copy()
                    pipeline_fold.publish(*base_fold, fold_apply)
                    stacker[fold_idx].subscribe(fold_apply.publisher)
                    merger[fold_idx].subscribe(base_fold.apply.publisher)
            return train_output, apply_output

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
