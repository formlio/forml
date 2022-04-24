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
Column manipulation actors.
"""
import abc
import logging
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow

from . import _convert

LOGGER = logging.getLogger(__name__)


class Concatenable(flow.Actor[flow.Features, None, flow.Result], metaclass=abc.ABCMeta):
    """Abstract concatenator of objects received on the input ports into single object."""

    def __init__(self, **kwargs):
        self._kwargs: dict[str, typing.Any] = kwargs

    def apply(self, *features: flow.Features) -> flow.Result:
        """Concat the individual objects into one dataframe.

        Args:
            features: Individual sources to be concatenated.

        Returns:
            Single concatenated dataframe.
        """
        return self.concat(features, **self._kwargs)

    @classmethod
    @abc.abstractmethod
    def concat(cls, features: typing.Sequence[flow.Features], **kwargs) -> flow.Result:
        """Concatenation logic implementation.

        Args:
            features: Sequence of features objects to be contatenated.
            **kwargs: Optional kwargs.

        Returns:
            Concatenated result.
        """
        raise NotImplementedError()

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return dict(self._kwargs)

    def set_params(self, **kwargs) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            kwargs: Concat kwargs.
        """
        self._kwargs.update(kwargs)


class PandasConcat(Concatenable[pdtype.NDFrame, pandas.DataFrame]):
    """Concat implementation based on Pandas Dataframe."""

    def __init__(self, axis: str = 'index'):
        super().__init__(axis=axis)

    @_convert.pandas_params
    def apply(self, *features: pdtype.NDFrame) -> pandas.DataFrame:
        return super().apply(*features)

    @classmethod
    def concat(cls, features: typing.Sequence[pdtype.NDFrame], **kwargs) -> pandas.DataFrame:
        """Concat the individual objects into one dataframe.

        Args:
            features: Individual sources to be concatenated.

        Returns:
            Single concatenated dataframe.
        """
        return pandas.concat(features, axis=kwargs.get('axis'), ignore_index=True)


class Apply(flow.Actor[flow.Features, None, flow.Features]):
    """Generic source apply actor."""

    def __init__(self, function: typing.Callable[[flow.Features], flow.Features]):
        self.function: typing.Callable[[flow.Features], flow.Features] = function

    @_convert.pandas_params
    def apply(self, *features: flow.Features) -> flow.Features:  # pylint: disable=arguments-differ
        """Execute the provided method with the given sources.

        Args:
            features: Inputs to be passed through the provided method.

        Returns:
            Transformed output as returned by the provided method.
        """
        return self.function(*features)


class LabelMergeable(flow.Actor[flow.Features, flow.Labels, flow.Features], metaclass=abc.ABCMeta):
    """Abstract label merging actor - inserting the labels as a new column to the feature set."""

    def __init__(self, **kwargs):
        self._kwargs: dict[str, typing.Any] = kwargs
        self._labels: typing.Optional[flow.Labels] = None

    def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
        """Train the inserter by remembering the labels.

        Args:
            features: X table.
            labels: Y series.
        """
        self._labels = labels

    def apply(self, features: flow.Features) -> flow.Features:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input data set.

        Returns:
            Features with label column removed plus just the label column in second new dataset.
        """
        if self._labels is None:
            raise RuntimeError('Merger not trained')
        return self.merge(features, self._labels, **self._kwargs)

    @classmethod
    @abc.abstractmethod
    def merge(cls, features: flow.Features, labels: flow.Labels, **kwargs) -> flow.Features:
        """Actual label merging logic.

        Args:
            features: X table.
            labels: Y series.

        Returns:
            Table with the label merged.
        """
        raise NotImplementedError()

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return dict(self._kwargs)

    def set_params(self, **kwargs) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            kwargs: Concat kwargs.
        """
        self._kwargs.update(kwargs)


class PandasLabelMerger(LabelMergeable[pandas.DataFrame, pandas.Series]):
    """Label-extraction inversion - inserting the labels as a new column to the feature set."""

    def __init__(self, label_header: str = 'Label'):
        super().__init__(label_header=label_header)

    @_convert.pandas_params
    def train(self, features: pandas.DataFrame, labels: pandas.Series, /) -> None:
        super().train(features, labels)

    @_convert.pandas_params
    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:
        return super().apply(features)

    @classmethod
    def merge(cls, features: pandas.DataFrame, labels: pandas.Series, **kwargs) -> pandas.DataFrame:
        return features.set_index(labels.rename(kwargs.get('label_header'))).reset_index()
