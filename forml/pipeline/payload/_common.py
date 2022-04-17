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

import logging
import typing

import pandas
from pandas.core import generic as pdtype

from forml import flow

from . import _format

LOGGER = logging.getLogger(__name__)


class Concat(flow.Actor[pdtype.NDFrame, None, pandas.DataFrame]):
    """Concatenate objects received on the input ports into single dataframe."""

    def __init__(self, axis: str = 'index'):
        self.axis: str = axis

    @_format.pandas_params
    def apply(self, *source: pdtype.NDFrame) -> pandas.DataFrame:
        """Concat the individual objects into one dataframe.

        Args:
            *source: Individual sources to be concatenated.

        Returns:
            Single concatenated dataframe.
        """
        return pandas.concat(source, axis=self.axis, ignore_index=True)


class Apply(flow.Actor[pdtype.NDFrame, None, pdtype.NDFrame]):
    """Generic source apply actor."""

    def __init__(self, function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame]):
        self.function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame] = function

    @_format.pandas_params
    def apply(self, *source: pdtype.NDFrame) -> pdtype.NDFrame:  # pylint: disable=arguments-differ
        """Execute the provided method with the given sources.

        Args:
            source: Inputs to be passed through the provided method.

        Returns:
            Transformed output as returned by the provided method.
        """
        return self.function(*source)


class ColumnExtractor(flow.Actor[pandas.DataFrame, None, tuple[pandas.DataFrame, pandas.DataFrame]]):
    """Column based label-extraction actor with 1:2 shape."""

    def __init__(self, column: str = 'label'):
        self.column: str = column

    @_format.pandas_params
    def apply(
        self, features: pandas.DataFrame
    ) -> tuple[pandas.DataFrame, pandas.Series]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input features set.

        Returns:
            Features with label column removed plus just the label column in second new dataset.
        """
        return features.drop(columns=self.column), features[self.column]

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return {'column': self.column}

    def set_params(self, column: str) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            column: Label column name.
        """
        self.column = column


class LabelMerger(flow.Actor[pandas.DataFrame, pandas.Series, pandas.DataFrame]):
    """Label-extraction inversion - inserting a label as a new column to the feature set."""

    def __init__(self, column: str = 'label'):
        self.column: str = column
        self._label: typing.Optional[pandas.Series] = None

    @_format.pandas_params
    def train(self, features: pandas.DataFrame, labels: pandas.Series) -> None:
        """Train the inserter by remembering the labels.
        Args:
            features: X table.
            labels: Y series.
        """
        self._label = labels

    @_format.pandas_params
    def apply(self, features: pandas.DataFrame) -> pandas.DataFrame:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input data set.

        Returns:
            Features with label column removed plus just the label column in second new dataset.
        """
        if self._label is None or len(features) != len(self._label):
            raise RuntimeError('Inserter not trained')
        return features.set_index(self._label.rename(self.column)).reset_index()

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return {'column': self.column}

    def set_params(self, column: str) -> None:  # pylint: disable=arguments-differ
        """Standard params setter.

        Args:
            column: Label column name.
        """
        self.column = column
