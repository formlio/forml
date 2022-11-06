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

from forml import flow

from .. import wrap
from . import _convert

LOGGER = logging.getLogger(__name__)


@wrap.Actor.apply
def Apply(  # pylint: disable=invalid-name
    *features: flow.Features, function: typing.Callable[..., flow.Features]
) -> flow.Features:
    """Apply(*, function: typing.Callable[..., flow.Features])

    Generic stateless function-based transformer actor.

    It can work with an arbitrary number of M:N input/output ports depending on the topology
    implemented by the owning operator.

    Args:
        function: Callable transformer to be applied to the data.

    Returns:
        Result of the *function* applied to the input data.

    Examples:
        >>> APPLY = payload.Apply(function=lambda df: df.dropna())
    """
    return function(*features)


@wrap.Actor.apply
def PandasConcat(*features: flow.Features, **kwargs) -> flow.Features:  # pylint: disable=invalid-name
    """PandasConcat(**kwargs)

    Stateless concatenation actor based on :func:`pandas:pandas.concat`.

    It plugs into a topology of *N:1* input/output ports.

    Args:
        kwargs: Any keywords accepted by :func:`pandas:pandas.concat`.

    Examples:
        >>> CONCAT = payload.PandasConcat(axis='columns', ignore_index=False)
    """
    return pandas.concat((_convert.pandas_read(f) for f in features), **({'copy': False} | kwargs))


@wrap.Actor.apply
def PandasSelect(  # pylint: disable=invalid-name
    features: pandas.DataFrame, *, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """PandasSelect(*, columns: typing.Sequence[str])

    Stateless mapper actor for :class:`pandas:pandas.DataFrame` column selection.

    Args:
        columns: Sequence of columns to be selected.

    Returns:
        New DataFrame with only the selected columns.

    Examples:
        >>> SELECT = payload.PandasSelect(columns=['foo', 'bar'])
    """
    return features[list(columns)]


@wrap.Actor.apply
def PandasDrop(  # pylint: disable=invalid-name
    features: pandas.DataFrame, *, columns: typing.Sequence[str]
) -> pandas.DataFrame:
    """PandasDrop(*, columns: typing.Sequence[str])

    Stateless mapper actor for :class:`pandas:pandas.DataFrame` column dropping.

    Args:
        columns: Sequence of columns to be dropped.

    Returns:
        New DataFrame without the dropped columns.

    Examples:
        >>> DROP = payload.PandasDrop(columns=['foo', 'bar'])
    """
    return features.drop(columns=list(columns))


class MapReduce(flow.Operator):
    """MapReduce(*mappers: flow.Builder, reducer: flow.Builder = PandasConcat.builder())

    Operator for applying parallel (possibly stateful) mapper actors and combining their outputs
    using a final (stateless) reducer.

    Args:
        mappers: Builders for individual mapper actors to be applied in parallel.
        reducer: Builder for a final reducer actor. The actor needs to accept as many inputs
                 as many mappers are provided. Defaults to :class:`payload.PandasConcat
                 <forml.pipeline.payload.PandasConcat>`.

    Examples:
        >>> MAPRED = payload.MapReduce(
        ...     payload.PandasSelect.builder(columns=['foo']),
        ...     payload.PandasDrop.builder(columns=['foo']),
        ... )
    """

    def __init__(
        self, *mappers: flow.Builder, reducer: flow.Builder = PandasConcat.builder(axis='columns')  # noqa: B008
    ):
        if reducer.actor.is_stateful():
            raise TypeError('Stateful reducer')
        if not mappers:
            raise ValueError('Mappers required')
        self._mappers: tuple[flow.Builder] = mappers
        self._reducer: flow.Builder = reducer

    def compose(self, scope: flow.Composable) -> flow.Trunk:
        left: flow.Trunk = scope.expand()
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
