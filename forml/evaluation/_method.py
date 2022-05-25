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

from sklearn import model_selection

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
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and a folding actor type.

        Args:
            crossvalidator: Implementation of the split selection logic.
            splitter: Folding actor type that is expected to take crossvalidator is its parameter.
                      Defaults to `payload.PandasCDFolds`.
        """

    @typing.overload
    def __init__(self, *, splitter: flow.Spec[payload.CVFoldable], nsplits: int):
        """CrossVal constructor based on splitter supplied in form of a Spec object.

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


class HoldOut(CrossVal):
    """Evaluation method base on a hold-out portion of the trainset.

    Implemented on top of the CrossVal method by simply forcing the number of folds to 1.
    """

    @typing.overload
    def __init__(
        self,
        *,
        test_size: typing.Optional[typing.Union[float, int]] = None,
        train_size: typing.Optional[typing.Union[float, int]] = None,
        random_state: typing.Optional[int] = None,
        stratify: bool = False,
        splitter: type[payload.CVFoldable] = payload.PandasCVFolds,
    ):
        """Simplified constructor based explicit test_size/train_size specifications that will be used to setup a
        ``StratifiedShuffleSplit`` or ``ShuffleSplit`` crossvalidator (depending on the ``stratify`` flag) for
        defining the splits.

        Args:
            test_size: Absolute (if int) or relative (if float) size of the test split (defaults to train_size
                       complement or 0.1).
            train_size: Absolute (if int) or relative (if float) size of the train split (defaults to train_size
                        complement or 0.1).
            random_state: Controls the randomness of the training and testing indices produced.
            stratify: Use ``StratifiedShuffleSplit`` if True otherwise use ``ShuffleSplit``.
            splitter: Folding actor type that is expected to take crossvalidator is its parameter.
                      Defaults to `payload.PandasCDFolds`.
        """

    @typing.overload
    def __init__(
        self,
        *,
        crossvalidator: payload.CrossValidable,
        splitter: type[payload.CVFoldable] = payload.PandasCVFolds,
    ):
        """Simplified constructor based on splitter supplied in form of a crossvalidator and a folding actor type.

        Args:
            crossvalidator: Implementation of the split selection logic.
            splitter: Folding actor type that is expected to take crossvalidator is its parameter.
                      Defaults to `payload.PandasCDFolds`.
        """

    @typing.overload
    def __init__(self, *, splitter: flow.Spec[payload.CVFoldable]):
        """HoldOut constructor based on splitter supplied in form of a Spec object.

        Args:
            splitter: Spec object defining the train-test splitter.
        """

    def __init__(
        self,
        *,
        test_size=None,
        train_size=None,
        random_state=None,
        stratify=None,
        crossvalidator=None,
        splitter: typing.Union[type[payload.CVFoldable], flow.Spec[payload.CVFoldable]] = payload.PandasCVFolds,
    ):
        if (test_size is None and train_size is None and random_state is None and stratify is None) ^ (
            crossvalidator is not None or isinstance(splitter, flow.Spec)
        ):
            raise TypeError('Invalid combination of crossvalidator and test_size/train_size/random_state/shuffle')

        cvsplits = None
        if not isinstance(splitter, flow.Spec):
            if not crossvalidator:
                cvclass = model_selection.StratifiedShuffleSplit if stratify else model_selection.ShuffleSplit
                crossvalidator = cvclass(test_size=test_size, train_size=train_size, random_state=random_state)
        else:
            cvsplits = 2
        super().__init__(crossvalidator=crossvalidator, splitter=splitter, nsplits=cvsplits)
        self._nsplits = 1  # force to single fold to avoid actual crossvalidation
