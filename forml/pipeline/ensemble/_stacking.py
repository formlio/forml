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
import inspect
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow as flowmod
from forml.pipeline import payload as paymod

if typing.TYPE_CHECKING:
    from forml import flow  # pylint: disable=reimported
    from forml.pipeline import payload  # pylint: disable=reimported


class Fold(collections.namedtuple('Fold', 'train, test')):
    """Holder of data splits belonging to the same fold."""

    class Train(typing.NamedTuple):
        """Train part of the fold."""

        apply: 'flow.Publishable'
        train: 'flow.Publishable'
        label: 'flow.Publishable'

    class Test(typing.NamedTuple):
        """Test part of the fold."""

        train: 'flow.Publishable'
        label: 'flow.Publishable'

    train: Train
    test: Test

    def __new__(
        cls,
        /,
        train_apply: 'flow.Publishable',
        train_train: 'flow.Publishable',
        train_label: 'flow.Publishable',
        test_train: 'flow.Publishable',
        test_label: 'flow.Publishable',
    ):
        return super().__new__(cls, cls.Train(train_apply, train_train, train_label), cls.Test(test_train, test_label))

    def publish(self, apply: 'flow.Segment', train: 'flow.Segment', label: 'flow.Segment', test: 'flow.Segment'):
        """Helper for connecting the individual data ports."""
        apply.subscribe(self.train.apply)
        train.subscribe(self.train.train)
        label.subscribe(self.train.label)
        test.subscribe(self.test.train)


class Ensembler(flowmod.Operator):
    """Abstract base class for stacking ensemblers."""

    class Builder(abc.ABC):
        """Implementation of fold stacking."""

        def __init__(self, bases: typing.Sequence['flow.Composable'], **kwargs):
            self._bases: tuple['flow.Composable'] = tuple(bases)
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __call__(self, folds: typing.Sequence[Fold]) -> tuple['flow.Node', 'flow.Node', 'flow.Node']:
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
            cls, bases: typing.Sequence['flow.Composable'], folds: typing.Sequence[Fold], **kwargs
        ) -> tuple['flow.Node', 'flow.Node', 'flow.Node']:
            """Stack the folds using the given bases and produce a tran and apply outputs.

            Args:
                bases: Sequence of the base model operators (or compositions) to be ensembled.
                folds: Sequence of the split data folds.
                **kwargs: Builder specific kwargs.

            Returns:
                Tuple of tail nodes returning the train, apply and label segment outputs.
            """

    @typing.overload
    def __init__(
        self,
        *bases: 'flow.Composable',
        crossvalidator: 'payload.CrossValidable',
        splitter: 'type[payload.CVFoldable]' = paymod.PandasCVFolds,
        **kwargs,
    ):
        """Simplified constructor based on splitter supplied in form of a cross-validator and
        a folding actor type.

        Args:
            *bases: Set of primary models to be ensembled.
            crossvalidator: Implementation of the split-selection logic.
            splitter: Folding actor type that is expected to take the *cross-validator* as its
                      parameter. Defaults to `payload.PandasCVFolds`.
        """

    @typing.overload
    def __init__(
        self,
        *bases: 'flow.Composable',
        splitter: 'flow.Builder[payload.CVFoldable]',
        nsplits: int,
        **kwargs,
    ):
        """Ensembler constructor based on splitter supplied in form of an actor builder object.

        Args:
            *bases: Set of primary models to be ensembled.
            splitter: Actor builder object defining the folding splitter.
            nsplits: The number of splits the splitter is going to generate (needs to be explicit as
                     there is no reliable way to extract it from the actor builder).
        """

    def __init__(
        self,
        *bases,
        crossvalidator=None,
        splitter=paymod.PandasCVFolds,
        nsplits=None,
        **kwargs,
    ):
        if not bases:
            raise ValueError('Base models required')
        if ((crossvalidator is None) ^ (nsplits is not None)) or (
            (crossvalidator is None) ^ isinstance(splitter, flowmod.Builder)
        ):
            raise TypeError('Invalid combination of crossvalidator, splitter and nsplits')
        if not isinstance(splitter, flowmod.Builder):
            splitter = splitter.builder(crossvalidator=crossvalidator)
            nsplits = crossvalidator.get_n_splits()
        if nsplits < 2:
            raise ValueError('At least 2 splits required')
        self._nsplits: int = nsplits
        self._splitter: 'flow.Builder[payload.CVFoldable]' = splitter
        self._builder: Ensembler.Builder = self.Builder(bases, **kwargs)  # pylint: disable=abstract-class-instantiated

    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        """Ensemble composition.

        Args:
            scope: left segment.

        Returns:
            Composed segment trunk.
        """
        head: 'flow.Trunk' = flowmod.Trunk()
        input_splitter = flowmod.Worker(self._splitter, 1, 2 * self._nsplits)
        input_splitter.train(head.train.publisher, head.label.publisher)
        feature_folds: 'flow.Worker' = input_splitter.fork()
        feature_folds[0].subscribe(head.train.publisher)
        label_folds: 'flow.Worker' = input_splitter.fork()
        label_folds[0].subscribe(head.label.publisher)

        data_folds = []
        for fid in range(self._nsplits):
            pipeline_fold: 'flow.Trunk' = scope.expand()
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
        return flowmod.Trunk(
            apply=head.apply.extend(tail=apply_tail),
            train=head.train.extend(tail=train_tail),
            label=head.label.extend(tail=label_tail),
        )


@paymod.pandas_params
def pandas_mean(*folds: pdtype.NDFrame) -> pandas.DataFrame:
    """Specific fold-models prediction reducer for Pandas dataframes based on arithmetic mean.

    Predictions can either be :class:`pandas:pandas.Series` or multicolumn
    :class:`pandas:pandas.DataFrame` in which case same-position columns are merged together.

    Args:
        *folds: Individual model predictions.

    Returns:
        Single dataframe with the mean value of the individual predictions representing the same
        event.
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
    """Stacking ensembler with N cross-validated instances of each base model - all of them also
    kept for serving.

    This operator actually only represents the first layer of the stacked ensembling topology -
    providing a derived training dataset as the stack of cross-validated predictions of the *base
    models*. This dataset is simply passed down to the next composed operator which should be the
    actual final stacking model constituting the second ensembling layer.

    The cross-validation splitter is prepended in front of the entire composition scope which is then
    expanded separately for every single fold creating N parallel branches cloned from the original
    segment.

    Instances of all stateful actors - including clones of the same logical entities within
    the parallel folds - are kept for serving (unlike with the other possible techniques where they
    get retrained on just a single instance each) where their individual predictions are combined
    using the ``reducer`` function (e.g. an arithmetical mean). This results in a computationally
    more expensive serving but potentially better accuracy.

    Args:
        bases: Sequence of the base model operators (or compositions) to be ensembled.
        crossvalidator: Implementation of the split-selection logic.
        splitter: Depending on the constructor version:

                  1. Folding actor type that is expected to take ``crossvalidator`` as its
                     parameter. Defaults to :class:`payload.PandasCVFolds
                     <forml.pipeline.payload.PandasCVFolds>`.
                  2. Actor builder instance defining the folding splitter.
        nsplits: The number of splits the splitter is going to generate (needs to be explicit as
                 there is no generic way to extract it from the actor builder).
        appender: Horizontal column concatenator (combining base model predictions in *train-mode*)
                  provided either as a *function* or as an actor *builder*.
        stacker: Vertical column concatenator (combining folds predictions in *train-mode*)
                 provided either as a *function* or as an actor *builder*.
        reducer: Horizontal column merger (combining base model predictions in *apply-mode*)
                 provided either as a *function* or as an actor *builder*.

    Examples:
            >>> PIPELINE = (
            ...     preprocessing.FooBar()
            ...     >> ensemble.FullStack(
            ...         sklearn.ensemble.GradientBoostingClassifier(),
            ...         sklearn.ensemble.RandomForestClassifier(),
            ...         crossvalidator=sklearn.model_selection.StratifiedKFold(n_splits=2))
            ...     >> sklearn.linear_model.LogisticRegression()
            ... )
    """

    class Builder(Ensembler.Builder):
        """Implementation of the fold stacking."""

        @classmethod
        def build(
            cls, bases: typing.Sequence['flow.Composable'], folds: typing.Sequence[Fold], **kwargs
        ) -> tuple['flow.Node', 'flow.Node', 'flow.Node']:
            nsplits = len(folds)
            label_output: 'flow.Worker' = flowmod.Worker(kwargs['stacker'], nsplits, 1)
            train_output: 'flow.Worker' = flowmod.Worker(kwargs['appender'], len(bases), 1)
            apply_output: 'flow.Worker' = train_output.fork()
            stacker_forks: typing.Iterable['flow.Worker'] = flowmod.Worker.fgen(kwargs['stacker'], nsplits, 1)
            reducer_forks: typing.Iterable['flow.Worker'] = flowmod.Worker.fgen(kwargs['reducer'], nsplits, 1)
            for fold_idx, pipeline_fold in enumerate(folds):
                label_output[fold_idx].subscribe(pipeline_fold.test.label)
            for base_idx, (base, stacker, reducer) in enumerate(zip(bases, stacker_forks, reducer_forks)):
                train_output[base_idx].subscribe(stacker[0])
                apply_output[base_idx].subscribe(reducer[0])
                for fold_idx, pipeline_fold in enumerate(folds):
                    base_fold: 'flow.Trunk' = base.expand()
                    fold_apply = base_fold.apply.copy()
                    pipeline_fold.publish(base_fold.apply, base_fold.train, base_fold.label, fold_apply)
                    stacker[fold_idx].subscribe(fold_apply.publisher)
                    reducer[fold_idx].subscribe(base_fold.apply.publisher)
            return train_output, apply_output, label_output

    @typing.overload
    def __init__(
        self,
        *bases: 'flow.Composable',
        crossvalidator: 'payload.CrossValidable',
        splitter: 'type[payload.CVFoldable]' = paymod.PandasCVFolds,
        appender: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = (
            paymod.PandasConcat.builder(axis='columns', ignore_index=False)  # noqa: B008
        ),
        stacker: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = (
            paymod.PandasConcat.builder(axis='index', ignore_index=True)  # noqa: B008
        ),
        reducer: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = pandas_mean,
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and
        a folding actor type.
        """

    @typing.overload
    def __init__(
        self,
        *bases: 'flow.Composable',
        splitter: 'flow.Builder[payload.CVFoldable]',
        nsplits: int,
        appender: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = (
            paymod.PandasConcat.builder(axis='columns', ignore_index=False)  # noqa: B008
        ),
        stacker: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = (
            paymod.PandasConcat.builder(axis='index', ignore_index=True)  # noqa: B008
        ),
        reducer: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]' = pandas_mean,
    ):
        """Ensembler constructor based on splitter supplied in form of an actor builder object."""

    def __init__(
        self,
        *bases,
        crossvalidator=None,
        splitter=paymod.PandasCVFolds,
        nsplits=None,
        appender=paymod.PandasConcat.builder(axis='columns', ignore_index=False),  # noqa: B008
        stacker=paymod.PandasConcat.builder(axis='index', ignore_index=True),  # noqa: B008
        reducer=pandas_mean,
    ):
        def ensure_builder(merger: 'typing.Union[typing.Callable[..., flow.Features], flow.Builder]') -> 'flow.Builder':
            """If the merger is provided as a plain function/method, wrap it using ``payload.Apply``."""
            if inspect.isfunction(merger) or inspect.ismethod(merger):
                merger = paymod.Apply.builder(function=merger)
            return merger

        super().__init__(
            *bases,
            crossvalidator=crossvalidator,
            splitter=splitter,
            nsplits=nsplits,
            appender=ensure_builder(appender),
            stacker=ensure_builder(stacker),
            reducer=ensure_builder(reducer),
        )
