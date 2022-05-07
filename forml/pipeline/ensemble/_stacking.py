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
Implementations of stacking ensemblers.
"""

import abc
import collections
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow
from forml.pipeline import payload


class Fold(collections.namedtuple('Fold', 'train, test')):
    """Holder of data splits belonging to the same fold."""

    class Train(typing.NamedTuple):
        """Train part of the fold."""

        apply: flow.Publishable
        train: flow.Publishable
        label: flow.Publishable

    class Test(typing.NamedTuple):
        """Test part of the fold."""

        train: flow.Publishable
        label: flow.Publishable

    train: Train
    test: Test

    def __new__(
        cls,
        /,
        train_apply: flow.Publishable,
        train_train: flow.Publishable,
        train_label: flow.Publishable,
        test_train: flow.Publishable,
        test_label: flow.Publishable,
    ):
        return super().__new__(cls, cls.Train(train_apply, train_train, train_label), cls.Test(test_train, test_label))

    def publish(self, apply: flow.Path, train: flow.Path, label: flow.Path, test: flow.Path):
        """Helper for connecting the individual data ports."""
        apply.subscribe(self.train.apply)
        train.subscribe(self.train.train)
        label.subscribe(self.train.label)
        test.subscribe(self.test.train)


class Ensembler(flow.Operator):
    """Base class for stacking ensemblers."""

    class Builder(abc.ABC):
        """Implementation of fold stacking."""

        def __init__(self, bases: typing.Sequence[flow.Composable], **kwargs):
            self._bases: tuple[flow.Composable] = tuple(bases)
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __call__(self, folds: typing.Sequence[Fold]) -> tuple[flow.Atomic, flow.Atomic, flow.Atomic]:
            """Stack the folds using the given bases and produce a tran and apply outputs.

            Args:
                folds: Sequence of the split data folds.

            Returns:
                Tuple of tail nodes returning the train and apply mode stack outputs.
            """
            return self.build(self._bases, folds, **self._kwargs)

        @classmethod
        @abc.abstractmethod
        def build(
            cls, bases: typing.Sequence[flow.Composable], folds: typing.Sequence[Fold], **kwargs
        ) -> tuple[flow.Atomic, flow.Atomic, flow.Atomic]:
            """Stack the folds using the given bases and produce a tran and apply outputs.

            Args:
                bases: Sequence of the base models to be ensembled.
                folds: Sequence of the split data folds.
                **kwargs: Builder specific kwargs.

            Returns:
                Tuple of tail nodes returning the train, apply and label path outputs.
            """

    @typing.overload
    def __init__(
        self,
        *bases: flow.Composable,
        crossvalidator: payload.CrossValidable,
        splitter: type[payload.CVFoldable] = payload.PandasCVFolds,
        **kwargs,
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and a folding actor type.


        Args:
            *bases: Set of primary models to be ensembled.
            crossvalidator: Implementation of the split selection logic.
            splitter: Folding actor type that is expected to take crossvalidator is its parameter.
                      Defaults to `payload.PandasCDFolds`.

        """

    @typing.overload
    def __init__(
        self,
        *bases: flow.Composable,
        splitter: flow.Spec[payload.CVFoldable],
        nsplits: int,
        **kwargs,
    ):
        """Ensembler constructor based on splitter supplied in form of a Spec object.

        Args:
            *bases: Set of primary models to be ensembled.
            splitter: Spec object defining the folding splitter.
            nsplits: Number of splits the splitter is going to generate (needs to be explicit as there is no reliable
                     way to extract it from the Spec).
        """

    def __init__(
        self,
        *bases,
        crossvalidator=None,
        splitter=payload.PandasCVFolds,
        nsplits=None,
        **kwargs,
    ):
        if not bases:
            raise ValueError('Base models required')
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
        self._builder: Ensembler.Builder = self.Builder(bases, **kwargs)  # pylint: disable=abstract-class-instantiated

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Ensemble composition.

        Args:
            left: left segment.

        Returns:
            Composed segment track.
        """
        head: flow.Trunk = flow.Trunk()
        input_splitter = flow.Worker(self._splitter, 1, 2 * self._nsplits)
        input_splitter.train(head.train.publisher, head.label.publisher)
        feature_folds: flow.Worker = input_splitter.fork()
        feature_folds[0].subscribe(head.train.publisher)
        label_folds: flow.Worker = input_splitter.fork()
        label_folds[0].subscribe(head.label.publisher)

        data_folds = []
        for fid in range(self._nsplits):
            pipeline_fold: flow.Trunk = left.expand()
            pipeline_fold.train.subscribe(feature_folds[2 * fid])
            pipeline_fold.label.subscribe(label_folds[2 * fid])
            pipeline_fold.apply.subscribe(head.apply.publisher)
            test_fold = pipeline_fold.apply.copy()
            test_fold.subscribe(feature_folds[2 * fid + 1])
            data_folds.append(
                Fold(
                    pipeline_fold.apply.publisher,
                    pipeline_fold.train.publisher,
                    pipeline_fold.label.publisher,
                    test_fold.publisher,
                    label_folds[2 * fid + 1],
                )
            )
        train_tail, apply_tail, label_tail = self._builder(data_folds)
        return flow.Trunk(
            apply=head.apply.extend(tail=apply_tail),
            train=head.train.extend(tail=train_tail),
            label=head.label.extend(tail=label_tail),
        )


@payload.pandas_params
def pandas_mean(*folds: pdtype.NDFrame) -> pandas.DataFrame:
    """Specific fold models prediction reducer for Pandas dataframes based on arithmetic mean.

    Predictions can either be series or multicolumn dataframes in which case same position columns are merged
    together.

    Args:
        *folds: Individual model predictions.

    Returns:
        Single dataframe with the mean value of the individual predictions.
    """
    if not (folds and all(f.shape == folds[0].shape for f in folds)):
        raise ValueError('Folds must have same shape')
    folds = (
        [([folds[0].name], folds)] if folds[0].ndim == 1 else (zip(*i) for i in zip(*(f.iteritems() for f in folds)))
    )
    return pandas.concat(
        (pandas.concat(s, axis='columns', copy=False).mean(axis='columns').rename(n[0]) for n, s in folds),
        axis='columns',
        copy=False,
    )


class FullStack(Ensembler):
    """Stacking ensembler with all individual models kept for serving."""

    class Builder(Ensembler.Builder):
        """Implementation of the fold stacking."""

        @classmethod
        def build(
            cls, bases: typing.Sequence[flow.Composable], folds: typing.Sequence[Fold], **kwargs
        ) -> tuple[flow.Atomic, flow.Atomic, flow.Atomic]:
            nsplits = len(folds)
            label_output: flow.Worker = flow.Worker(kwargs['stacker'], nsplits, 1)
            train_output: flow.Worker = flow.Worker(kwargs['appender'], len(bases), 1)
            apply_output: flow.Worker = train_output.fork()
            stacker_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(kwargs['stacker'], nsplits, 1)
            reducer_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(
                payload.Apply.spec(function=kwargs['reducer']), nsplits, 1
            )
            for fold_idx, pipeline_fold in enumerate(folds):
                label_output[fold_idx].subscribe(pipeline_fold.test.label)
            for base_idx, (base, stacker, reducer) in enumerate(zip(bases, stacker_forks, reducer_forks)):
                train_output[base_idx].subscribe(stacker[0])
                apply_output[base_idx].subscribe(reducer[0])
                for fold_idx, pipeline_fold in enumerate(folds):
                    base_fold: flow.Trunk = base.expand()
                    fold_apply = base_fold.apply.copy()
                    pipeline_fold.publish(base_fold.apply, base_fold.train, base_fold.label, fold_apply)
                    stacker[fold_idx].subscribe(fold_apply.publisher)
                    reducer[fold_idx].subscribe(base_fold.apply.publisher)
            return train_output, apply_output, label_output

    def __init__(
        self,
        *bases,
        crossvalidator=None,
        splitter=payload.PandasCVFolds,
        nsplits=None,
        appender: flow.Spec[payload.Concatenable] = payload.PandasConcat.spec(  # noqa: B008
            axis='columns', ignore_index=False
        ),
        stacker: flow.Spec[payload.Concatenable] = payload.PandasConcat.spec(  # noqa: B008
            axis='index', ignore_index=True
        ),
        reducer: typing.Callable[..., flow.Features] = pandas_mean,
    ):
        super().__init__(
            *bases,
            crossvalidator=crossvalidator,
            splitter=splitter,
            nsplits=nsplits,
            appender=appender,
            stacker=stacker,
            reducer=reducer,
        )
