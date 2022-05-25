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
Payload splitting functions.
"""
import abc
import logging
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow

from . import _convert

LOGGER = logging.getLogger(__name__)


Column = typing.TypeVar('Column')


class CrossValidable(typing.Protocol[flow.Features, flow.Labels, Column]):
    """Protocol for the cross-validator implementation.

    This matches for example all the SKLearn `sklearn.model_selection.BaseCrossValidator` implementations.
    """

    def split(
        self,
        features: flow.Features,
        labels: typing.Optional[flow.Labels] = None,
        groups: typing.Optional[Column] = None,
        /,
    ) -> typing.Iterable[tuple[typing.Sequence[int], typing.Sequence[int]]]:
        """Generate indices to split data into training and test set.

        Args:
            features: Train features data.
            labels: Target data.
            groups: Group labels.

        Returns:
            Iterable of tuples of train/test indexes.
        """

    def get_n_splits(
        self,
        features: flow.Features,
        labels: typing.Optional[flow.Labels] = None,
        groups: typing.Optional[Column] = None,
        /,
    ) -> int:
        """Returns the number of splitting iterations in the cross-validator.

        Args:
            features: Train features data.
            labels: Target data.
            groups: Group labels.

        Returns:
            Number of folds.
        """


class CVFoldable(
    typing.Generic[flow.Features, flow.Labels, Column],
    flow.Actor[flow.Features, flow.Labels, typing.Sequence[flow.Features]],
    metaclass=abc.ABCMeta,
):
    """Abstract n-folds splitter of train-test folds based on the provided cross-validator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.

    The splits are provided in a range of output ports where a given fold with index i is delivered via ports:
      * [2 * i]: trainset
      * [2 * i + 1]: testset
    """

    def __init__(
        self,
        crossvalidator: CrossValidable[flow.Features, flow.Labels, Column],
        groups_extractor: typing.Optional[typing.Callable[[flow.Features], Column]] = None,
    ):
        self._crossvalidator: CrossValidable[flow.Features, flow.Labels, Column] = crossvalidator
        self._groups_extractor: typing.Optional[typing.Callable[[flow.Features], Column]] = groups_extractor
        self._indices: typing.Optional[tuple[tuple[typing.Sequence[int], typing.Sequence[int]]]] = None

    def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
        """Train the splitter on the provided data.
        Args:
            features: X table.
            labels: Y series.
        """
        groups = self._groups_extractor(features) if self._groups_extractor else None
        self._indices = tuple(self._crossvalidator.split(features, labels, groups))  # tuple it so it can be pickled

    def apply(self, features: flow.Features) -> typing.Sequence[flow.Features]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input data set.

        Returns:
            Sequence of repeated train, test, train, test, ... split folds.
        """
        if not self._indices:
            raise RuntimeError('Splitter not trained')
        LOGGER.debug('Splitting into %d train-test folds', len(self._indices))
        return self.split(features, self._indices)

    @classmethod
    @abc.abstractmethod
    def split(
        cls, features: flow.Features, indices: typing.Sequence[tuple[typing.Sequence[int], typing.Sequence[int]]]
    ) -> typing.Sequence[flow.Features]:
        """Splitting implementation.

        Args:
            features: Source features to split.
            indices: Sequence of fold indices to split by.

        Returns:
            Sequence of repeated train, test, train, test, ... split folds.
        """
        raise NotImplementedError()

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return {'crossvalidator': self._crossvalidator}

    def set_params(self, crossvalidator: CrossValidable) -> None:
        """Standard params setter.

        Args:
            crossvalidator: New crossvalidator.
        """
        self._crossvalidator = crossvalidator


class PandasCVFolds(CVFoldable[pandas.DataFrame, pdtype.NDFrame, pandas.Series]):
    """Abstract n-folds splitter of train-test folds based on the provided cross-validator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.

    The splits are provided in a range of output ports where a given fold with index i is delivered via ports:
      * [2 * i]: trainset
      * [2 * i + 1]: testset
    """

    @_convert.pandas_params
    def train(self, features: pandas.DataFrame, labels: pdtype.NDFrame, /) -> None:
        super().train(features, labels)

    @_convert.pandas_params
    def apply(self, features: pandas.DataFrame) -> typing.Sequence[pandas.DataFrame]:
        return super().apply(features)

    @classmethod
    def split(
        cls, features: pandas.DataFrame, indices: typing.Sequence[tuple[typing.Sequence[int], typing.Sequence[int]]]
    ) -> typing.Sequence[pandas.DataFrame]:
        return tuple(
            s
            for a, b in indices
            for s in (features.iloc[a].reset_index(drop=True), features.iloc[b].reset_index(drop=True))
        )
