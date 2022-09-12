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
Evaluation method implementations.
"""
import typing

from sklearn import model_selection

from forml import flow
from forml.pipeline import payload as paymod

from . import _api

if typing.TYPE_CHECKING:
    from forml import evaluation
    from forml.pipeline import payload  # pylint: disable=reimported


class CrossVal(_api.Method):
    """Evaluation method based on a number of independent train-test trials using different parts of
    the same training dataset.

    The training dataset gets split into multiple (possibly overlaying) train-test pairs (folds)
    used to train a vanilla instance of the pipeline and to pass down *predictions* along with
    *true* outcomes independently for each fold.

    Args:
        crossvalidator: Implementation of the split-selection logic.
        splitter: Depending on the constructor version:

                  1. Folding actor type that is expected to take the *cross-validator* as its
                     parameter. Defaults to :class:`payload.PandasCVFolds
                     <forml.pipeline.payload.PandasCVFolds>`.
                  2. Actor builder instance defining the folding splitter.
        nsplits: The number of splits the splitter is going to generate (needs to be explicit as
                 there is no generic way to infer it from the Builder).

    Examples:
        >>> CROSSVAL = evaluation.CrossVal(
        ...     crossvalidator=sklearn.model_selection.StratifiedKFold(
        ...         n_splits=3, shuffle=True, random_state=42
        ...     )
        ... )
    """

    @typing.overload
    def __init__(
        self,
        *,
        crossvalidator: 'payload.CrossValidable',
        splitter: 'type[payload.CVFoldable]' = paymod.PandasCVFolds,
    ):
        """Constructor based on a splitter supplied in form of a cross-validator and a folding
        actor type.

        Args:
            crossvalidator: Implementation of the split-selection logic.
            splitter: Folding actor type that is expected to take the *cross-validator* is its
                      parameter. Defaults to :class:`payload.PandasCVFolds
                      <forml.pipeline.payload.PandasCVFolds>`.
        """

    @typing.overload
    def __init__(self, *, splitter: 'flow.Builder[payload.CVFoldable]', nsplits: int):
        """Constructor based on a ``splitter`` supplied in form of an actor *builder* instance.

        Args:
            splitter: Actor builder instance defining the folding splitter.
            nsplits: Number of splits the splitter is going to generate (needs to be explicit as
                     there is no generic way to infer it from the Builder).
        """

    def __init__(self, *, crossvalidator=None, splitter=paymod.PandasCVFolds, nsplits=None):
        if ((crossvalidator is None) ^ (nsplits is not None)) or (
            (crossvalidator is None) ^ isinstance(splitter, flow.Builder)
        ):
            raise TypeError('Invalid combination of crossvalidator, splitter and nsplits')
        if not isinstance(splitter, flow.Builder):
            splitter = splitter.builder(crossvalidator=crossvalidator)
            nsplits = crossvalidator.get_n_splits()
        if nsplits < 2:
            raise ValueError('At least 2 splits required')
        self._nsplits: int = nsplits
        self._splitter: flow.Builder['payload.CVFoldable'] = splitter

    def produce(
        self, pipeline: flow.Composable, features: flow.Publishable, labels: flow.Publishable
    ) -> typing.Iterable['evaluation.Outcome']:
        splitter = flow.Worker(self._splitter, 1, 2 * self._nsplits)
        splitter.train(features, labels)

        features_splitter: flow.Worker = splitter.fork()
        features_splitter[0].subscribe(features)
        labels_splitter: flow.Worker = splitter.fork()
        labels_splitter[0].subscribe(labels)

        outcomes = []
        for fid in range(self._nsplits):
            fold: flow.Trunk = pipeline.expand()
            fold.train.subscribe(features_splitter[2 * fid])
            fold.label.subscribe(labels_splitter[2 * fid])
            fold.apply.subscribe(features_splitter[2 * fid + 1])
            outcomes.append(_api.Outcome(labels_splitter[2 * fid + 1].publisher, fold.apply.publisher))
        return tuple(outcomes)


class HoldOut(CrossVal):
    """Evaluation method based on part of a training dataset being withheld for testing the
    predictions.

    The historical dataset available for evaluation is first split into two parts, one is used
    for training the pipeline, and the second for making actual *predictions* which are then exposed
    together with the *true* outcomes for eventual scoring.

    Note:
        This is implemented on top of the :class:`evaluation.CrossVal <forml.evaluation.CrossVal>`
        method simply by forcing the number of folds to 1.

    Args:
        test_size: Absolute (if ``int``) or relative (if ``float``) size of the test split
                   (defaults to ``train_size`` complement or ``0.1``).
        train_size: Absolute (if ``int``) or relative (if ``float``) size of the train split
                    (defaults to ``test_size`` complement).
        random_state: Controls the randomness of the training and testing indices produced.
        stratify: Use :class:`StratifiedShuffleSplit
                  <sklearn:sklearn.model_selection.StratifiedShuffleSplit>` if ``True`` otherwise
                  use :class:`ShuffleSplit <sklearn.model_selection.ShuffleSplit>`.
        crossvalidator: Implementation of the split-selection logic.
        splitter: Depending on the constructor version:

                  1. The folding actor type that is expected to take the *cross-validator* is its
                     parameter. Defaults to :class:`payload.PandasCVFolds
                     <forml.pipeline.payload.PandasCVFolds>`.
                  2. Actor builder instance defining the train-test splitter.

    Examples:
        >>> HOLDOUT = evaluation.HoldOut(test_size=0.2, stratify=True, random_state=42)
    """

    @typing.overload
    def __init__(
        self,
        *,
        test_size: typing.Optional[typing.Union[float, int]] = None,
        train_size: typing.Optional[typing.Union[float, int]] = None,
        random_state: typing.Optional[int] = None,
        stratify: bool = False,
        splitter: 'type[payload.CVFoldable]' = paymod.PandasCVFolds,
    ):
        """Constructor based explicit ``test_size``/``train_size`` specifications that will be used
        to setup a :class:`StratifiedShuffleSplit
        <sklearn:sklearn.model_selection.StratifiedShuffleSplit>` or :class:`ShuffleSplit
        <sklearn.model_selection.ShuffleSplit>` cross-validator (depending on the ``stratify`` flag)
        for defining the splits.

        Args:
            test_size: Absolute (if ``int``) or relative (if ``float``) size of the test split
                       (defaults to ``train_size`` complement or ``0.1``).
            train_size: Absolute (if ``int``) or relative (if ``float``) size of the train split
                        (defaults to ``test_size`` complement).
            random_state: Controls the randomness of the training and testing indices produced.
            stratify: Use :class:`StratifiedShuffleSplit
                      <sklearn:sklearn.model_selection.StratifiedShuffleSplit>` if True otherwise
                      use :class:`ShuffleSplit <sklearn.model_selection.ShuffleSplit>`.
            splitter: Folding actor type that is expected to take a *cross-validator* is its
                      parameter. Defaults to :class:`payload.PandasCVFolds
                      <forml.pipeline.payload.PandasCVFolds>`.
        """

    @typing.overload
    def __init__(
        self,
        *,
        crossvalidator: 'payload.CrossValidable',
        splitter: 'type[payload.CVFoldable]' = paymod.PandasCVFolds,
    ):
        """Constructor based on a splitter supplied in form of a cross-validator and a folding
        actor type.

        Args:
            crossvalidator: Implementation of the split-selection logic.
            splitter: Folding actor type that is expected to take the *cross-validator* is its
                      parameter. Defaults to :class:`payload.PandasCVFolds
                      <forml.pipeline.payload.PandasCVFolds>`.
        """

    @typing.overload
    def __init__(self, *, splitter: 'flow.Builder[payload.CVFoldable]'):
        """Constructor based on a ``splitter`` supplied in form of an actor *builder* instance.

        Args:
            splitter: Actor builder instance defining the train-test splitter.
        """

    def __init__(
        self,
        *,
        test_size=None,
        train_size=None,
        random_state=None,
        stratify=None,
        crossvalidator=None,
        splitter=paymod.PandasCVFolds,
    ):
        if (test_size is None and train_size is None and random_state is None and stratify is None) ^ (
            crossvalidator is not None or isinstance(splitter, flow.Builder)
        ):
            raise TypeError('Invalid combination of crossvalidator and test_size/train_size/random_state/shuffle')

        cvsplits = None
        if not isinstance(splitter, flow.Builder):
            if not crossvalidator:
                cvclass = model_selection.StratifiedShuffleSplit if stratify else model_selection.ShuffleSplit
                crossvalidator = cvclass(test_size=test_size, train_size=train_size, random_state=random_state)
        else:
            cvsplits = 2
        super().__init__(crossvalidator=crossvalidator, splitter=splitter, nsplits=cvsplits)
        self._nsplits = 1  # force to single fold to avoid actual crossvalidation
