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
Label manipulation actors.
"""
import logging
import typing

import pandas

from forml.flow import task
from forml.lib.flow.actor import ndframe

LOGGER = logging.getLogger(__name__)


class ColumnExtractor(task.Actor):
    """Column based label-extraction actor with 1:2 shape."""

    def __init__(self, column: str = 'label'):
        self.column: str = column

    @ndframe.auto
    def apply(
        self, features: pandas.DataFrame
    ) -> typing.Tuple[pandas.DataFrame, pandas.Series]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            features: Input features set.

        Returns:
            Features with label column removed plus just the label column in second new dataset.
        """
        return features.drop(columns=self.column), features[self.column]

    def get_params(self) -> typing.Dict[str, typing.Any]:
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


class ColumnInserter(task.Actor):
    """Label-extraction inversion - inserting a label as a new column to the feature set."""

    def __init__(self, column: str = 'label'):
        self.column: str = column
        self._label: typing.Optional[pandas.Series] = None

    @ndframe.auto
    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the inserter by remembering the labels.
        Args:
            features: X table.
            label: Y series.
        """
        self._label = label

    @ndframe.auto
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

    def get_params(self) -> typing.Dict[str, typing.Any]:
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
