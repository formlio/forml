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
Dataframe conversion tools.
"""
import functools
import logging
import typing

import numpy
import pandas
from pandas.core import generic as pdtype

from forml import flow
from forml.pipeline import topology

LOGGER = logging.getLogger(__name__)


def _ndf(data: typing.Any, columns: typing.Optional[typing.Sequence[str]] = None) -> pdtype.NDFrame:
    """Conversion logic.

    Args:
        data: Argument to be converted.
        columns: Optional column names.

    Returns:
        Converted pandas object.
    """

    def from_rows() -> pandas.DataFrame:
        """Helper conversion from list of rows."""
        return pandas.DataFrame(data, columns=columns).infer_objects()

    def from_vector() -> pandas.Series:
        """Helper conversion for a single series."""
        return pandas.Series(data).infer_objects()

    if isinstance(data, pdtype.NDFrame):
        return data
    if isinstance(data, numpy.ndarray):
        return from_vector() if data.ndim == 1 else from_rows()
    if isinstance(data, (tuple, list)):
        return from_vector() if data and not isinstance(data[0], (tuple, list)) else from_rows()
    LOGGER.warning('Unknown NDFrame conversion strategy for %s: %.1024s', type(data), data)
    return data


def pandas_params(
    wrapped: typing.Callable[[flow.Actor, pdtype.NDFrame], typing.Any]
) -> typing.Callable[[flow.Actor, typing.Any], typing.Any]:
    """Decorator for converting input parameters and return value to pandas.

    Args:
        wrapped: Actor method to be decorated.

    Returns:
        Decorated method.
    """

    @functools.wraps(wrapped)
    def wrapper(self: flow.Actor, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        """Decorating wrapper.

        Args:
            self: Actor self.
            *args: Input arguments to be converted.
            **kwargs: Input key-word arguments.

        Returns:
            Original output.
        """
        return wrapped(self, *(_ndf(a) for a in args), **{k: _ndf(v) for k, v in kwargs.items()})

    return wrapper


@topology.Adapter.apply
@topology.Adapter.train
@topology.Adapter.label
@topology.Function.actor
def to_pandas(data: typing.Any, columns: typing.Optional[typing.Sequence[str]] = None) -> pdtype.NDFrame:
    """Simple 1:1 operator that attempts to convert the data on each of apply/train/label path to pandas
    dataframe/series.

    Args:
        data: Input data.
        columns: Optional column names.

    Returns:
        Pandas dataframe/series.
    """
    return _ndf(data, columns=columns)
