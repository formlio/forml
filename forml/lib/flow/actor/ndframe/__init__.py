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
Dataframe manipulation actors.

The implementation is not enforcing any consistency (in terms of number of inputs or their shapes etc).
"""
import functools
import itertools
import logging
import typing

import numpy
import pandas
from pandas.core import generic as pdtype
from sklearn import model_selection

from forml.flow import task

LOGGER = logging.getLogger(__name__)


def cast(data: typing.Any, columns: typing.Optional[typing.Sequence[str]] = None) -> pdtype.NDFrame:
    """Conversion logic.

    Args:
        data: Argument to be converted.
        columns: Optional column names.

    Returns:
        Converted pandas object.
    """
    if isinstance(data, pdtype.NDFrame):
        return data
    if isinstance(data, numpy.ndarray):
        return pandas.Series(data) if data.ndim == 1 else pandas.DataFrame(data, columns=columns)
    if isinstance(data, (tuple, list)):
        if data and not isinstance(data[0], (tuple, list)):
            return pandas.Series(data)
        return pandas.concat(
            (pandas.Series(d, name=n) for d, n in itertools.zip_longest(data, columns or [])), axis='columns'
        )
    LOGGER.warning('Unknown NDFrame conversion strategy for %s: %.1024s', type(data), data)
    return data


def auto(
    wrapped: typing.Callable[[task.Actor, pdtype.NDFrame], typing.Any]
) -> typing.Callable[[task.Actor, typing.Any], typing.Any]:
    """Decorator for converting input parameters and return value to pandas.

    Args:
        wrapped: Actor method to be decorated.

    Returns:
        Decorated method.
    """

    @functools.wraps(wrapped)
    def wrapper(self: task.Actor, *args: typing.Any) -> typing.Any:
        """Decorating wrapper.

        Args:
            self: Actor self.
            *args: Input arguments to be converted.

        Returns:
            Original output.
        """
        return wrapped(self, *(cast(a) for a in args))

    return wrapper


class TrainTestSplit(task.Actor):
    """Train-test splitter generation n-folds of train-test splits based on the provided crossvalidator.

    The actor keeps all the generated indices as its internal state so that it can be used repeatedly for example to
    split data and labels independently.
    """

    def __init__(self, crossvalidator: model_selection.BaseCrossValidator):
        self.crossvalidator: model_selection.BaseCrossValidator = crossvalidator
        self._indices: typing.Optional[typing.Tuple[typing.Tuple[typing.Sequence[int], typing.Sequence[int]]]] = None

    def train(self, features: pandas.DataFrame, label: pandas.Series) -> None:
        """Train the splitter on the provided data.
        Args:
            features: X table.
            label: Y series.
        """
        self._indices = tuple(self.crossvalidator.split(features, label))  # tuple it so it can be pickled

    @auto
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

    def get_params(self) -> typing.Dict[str, typing.Any]:
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


class Concat(task.Actor):
    """Concatenate objects received on the input ports into single dataframe."""

    def __init__(self, axis: str = 'index'):
        self.axis: str = axis

    @auto
    def apply(self, *source: pdtype.NDFrame) -> pandas.DataFrame:
        """Concat the individual objects into one dataframe.

        Args:
            *source: Individual sources to be concatenated.

        Returns:
            Single concatenated dataframe.
        """
        return pandas.concat(source, axis=self.axis, ignore_index=True)


class Apply(task.Actor):
    """Generic source apply actor."""

    def __init__(self, function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame]):
        self.function: typing.Callable[[pdtype.NDFrame], pdtype.NDFrame] = function

    @auto
    def apply(self, *source: pdtype.NDFrame) -> pdtype.NDFrame:  # pylint: disable=arguments-differ
        """Execute the provided method with the given sources.

        Args:
            source: Inputs to be passed through the provided method.

        Returns:
            Transformed output as returned by the provided method.
        """
        return self.function(*source)
