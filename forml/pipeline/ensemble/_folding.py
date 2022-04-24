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

    class Builder(abc.ABC):
        """Implementation of fold stacking."""

        def __init__(self, bases: typing.Sequence[flow.Composable], **kwargs):
            self._bases: tuple[flow.Composable] = tuple(bases)
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __call__(self, folds: typing.Sequence[Split]) -> tuple[flow.Atomic, flow.Atomic]:
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
            cls, bases: typing.Sequence[flow.Composable], folds: typing.Iterable[Split], **kwargs
        ) -> tuple[flow.Atomic, flow.Atomic]:
            """Stack the folds using the given bases and produce a tran and apply outputs.

            Args:
                bases: Sequence of the base models to be ensembled.
                folds: Sequence of the split data folds.
                **kwargs: Builder specific kwargs.

            Returns:
                Tuple of tail nodes returning the train and apply mode stack outputs.
            """

    @typing.overload
    def __init__(
        self,
        *bases: flow.Composable,
        crossvalidator: payload.CrossValidable,
        splitter: type[payload.CVFoldable] = payload.PandasCVFolds,
        nsplits: None = None,
        **kwargs,
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and a folding actor type.

        Parameter nsplits must not be provided.

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
        crossvalidator: None = None,
        splitter: flow.Spec[payload.CVFoldable],
        nsplits: int,
        **kwargs,
    ):
        """Ensembler constructor based on splitter supplied in form of a Spec object.

        Crossvalidator must not be provided.

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
        if (crossvalidator is None) ^ (nsplits is not None) ^ isinstance(splitter, flow.Spec):
            raise TypeError('Invalid combination of crossvalidator, splitter and nsplits')
        if not isinstance(splitter, flow.Spec):
            splitter = splitter.spec(crossvalidator=crossvalidator)
            nsplits = crossvalidator.get_n_splits()
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
                Split(
                    pipeline_fold.apply.publisher,
                    pipeline_fold.train.publisher,
                    pipeline_fold.label.publisher,
                    test_fold.publisher,
                )
            )
        train_tail, apply_tail = self._builder(data_folds)
        return head.use(apply=head.apply.extend(tail=apply_tail), train=head.train.extend(tail=train_tail))


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
        [([f.name for f in folds], folds)]
        if folds[0].ndim == 1
        else (zip(*i) for i in zip(*(f.iteritems() for f in folds)))
    )
    return pandas.concat(
        (pandas.concat(s, axis='columns').mean(axis='columns').rename(n[0]) for n, s in folds), axis='columns'
    )


class FullStack(Ensembler):
    """Stacking ensembler with all individual models kept for serving."""

    class Builder(Ensembler.Builder):
        """Implementation of the fold stacking."""

        @classmethod
        def build(
            cls, bases: typing.Sequence[flow.Composable], folds: typing.Iterable[Split], **kwargs
        ) -> tuple[flow.Atomic, flow.Atomic]:
            folds = tuple(folds)
            nsplits = len(folds)
            train_output: flow.Worker = flow.Worker(kwargs['appender'], len(bases), 1)
            apply_output: flow.Worker = train_output.fork()
            stacker_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(kwargs['stacker'], nsplits, 1)
            merger_forks: typing.Iterable[flow.Worker] = flow.Worker.fgen(
                payload.Apply.spec(function=kwargs['reducer']), nsplits, 1
            )
            for base_idx, (base, stacker, merger) in enumerate(zip(bases, stacker_forks, merger_forks)):
                train_output[base_idx].subscribe(stacker[0])
                apply_output[base_idx].subscribe(merger[0])
                for fold_idx, pipeline_fold in enumerate(folds):
                    base_fold: flow.Trunk = base.expand()
                    fold_apply = base_fold.apply.copy()
                    pipeline_fold.publish(base_fold.apply, base_fold.train, base_fold.label, fold_apply)
                    stacker[fold_idx].subscribe(fold_apply.publisher)
                    merger[fold_idx].subscribe(base_fold.apply.publisher)
            return train_output, apply_output

    def __init__(
        self,
        *bases,
        crossvalidator=None,
        splitter=payload.PandasCVFolds,
        nsplits=None,
        appender: flow.Spec[payload.Concatenable] = payload.PandasConcat.spec(axis='columns'),  # noqa: B008
        stacker: flow.Spec[payload.Concatenable] = payload.PandasConcat.spec(axis='index'),  # noqa: B008
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
