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

from .. import wrap
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
            features: Sequence of features objects to be concatenated.
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

    DEFAULTS = {'copy': False}

    def __init__(self, **kwargs):
        super().__init__(**(self.DEFAULTS | kwargs))

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
        return pandas.concat(features, **kwargs)


@wrap.Actor.apply
def Apply(  # pylint: disable=invalid-name
    *features: flow.Features, function: typing.Callable[..., flow.Features]
) -> pandas.DataFrame:
    """Generic source apply actor."""
    return function(*features)


@wrap.Actor.apply
def PandasSelect(  # pylint: disable=invalid-name
    features: pandas.DataFrame, *, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """Column selection actor implementation based on Pandas Dataframe."""
    return features[list(columns)]


@wrap.Actor.apply
def PandasDrop(  # pylint: disable=invalid-name
    features: pandas.DataFrame, *, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """Column drop actor implementation based on Pandas Dataframe."""
    return features.drop(columns=list(columns))


class MapReduce(flow.Operator):
    """Operator for applying parallel (possibly stateful) mapper actors and a final (stateless) reducer."""

    def __init__(self, *mappers: flow.Spec, reducer: flow.Spec = PandasConcat.spec(axis='columns')):  # noqa: B008
        if reducer.actor.is_stateful():
            raise TypeError('Stateful reducer')
        if not mappers:
            raise ValueError('Mappers required')
        self._mappers: tuple[flow.Spec] = mappers
        self._reducer: flow.Spec = reducer

    def compose(self, left: flow.Composable) -> flow.Trunk:
        left: flow.Trunk = left.expand()
        apply_reducer = flow.Worker(self._reducer, len(self._mappers), 1)
        train_reducer = apply_reducer.fork()
        for idx, mapper in enumerate(self._mappers):
            apply_applier = flow.Worker(mapper, 1, 1)
            apply_applier[0].subscribe(left.apply.publisher)
            train_applier = apply_applier.fork()
            train_applier[0].subscribe(left.train.publisher)
            if mapper.actor.is_stateful():
                train_trainer = apply_applier.fork()
                train_trainer.train(left.train.publisher, left.label.publisher)
            apply_reducer[idx].subscribe(apply_applier[0])
            train_reducer[idx].subscribe(train_applier[0])
        return left.use(apply=left.apply.extend(tail=apply_reducer), train=left.train.extend(tail=train_reducer))
