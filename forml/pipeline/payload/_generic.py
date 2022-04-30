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
