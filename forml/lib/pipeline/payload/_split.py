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
import logging
import typing

import pandas
from sklearn import model_selection

from forml import flow
from forml.lib.pipeline.payload import _format

LOGGER = logging.getLogger(__name__)


class CVFolds(flow.Actor):
    """Train-test splitter generation n-folds of train-test splits based on the provided crossvalidator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.

    The splits are provided in a range of output ports where a given fold with index i is delivered via ports:
      * [2 * i]: trainset
      * [2 * i + 1]: testset
    """

    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self.crossvalidator: model_selection.BaseCrossValidator = crossvalidator
        self._indices: typing.Optional[tuple[tuple[typing.Sequence[int], typing.Sequence[int]]]] = None

    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the splitter on the provided data.
        Args:
            features: X table.
            label: Y series.
        """
        self._indices = tuple(self.crossvalidator.split(features, label))  # tuple it so it can be pickled

    @_format.pandas_params
    def apply(self, source: pandas.DataFrame) -> typing.Sequence[pandas.DataFrame]:  # pylint: disable=arguments-differ
        """Transforming the input feature set into two outputs separating the label column into the second one.

        Args:
            source: Input data set.

        Returns:
            Features with label column removed plus just the label column in second new dataset.
        """
        if not self._indices:
            raise RuntimeError('Splitter not trained')
        LOGGER.debug('Splitting %d rows into %d train-test sets', len(source), len(self._indices))
        return tuple(s for a, b in self._indices for s in (source.iloc[a], source.iloc[b]))

    def get_params(self) -> dict[str, typing.Any]:
        """Standard param getter.

        Returns:
            Actor params.
        """
        return {'crossvalidator': self.crossvalidator}

    def set_params(
        self, crossvalidator: model_selection.BaseCrossValidator  # pylint: disable=arguments-differ
    ) -> None:
        """Standard params setter.

        Args:
            crossvalidator: New crossvalidator.
        """
        self.crossvalidator = crossvalidator
