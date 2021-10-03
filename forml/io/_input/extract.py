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
Extract utilities.
"""
import abc
import logging
import typing

import forml
from forml import flow
from forml.io import dsl, layout
from forml.io.dsl import parser as parsmod

LOGGER = logging.getLogger(__name__)


class Statement(typing.NamedTuple):
    """Select statement defined as a query and definition of the ordinal expression."""

    prepared: 'Prepared'
    lower: typing.Optional[dsl.Native]
    upper: typing.Optional[dsl.Native]

    class Prepared(typing.NamedTuple):
        """Statement bound with particular lower/upper parameters."""

        query: dsl.Query
        ordinal: typing.Optional[dsl.Operable]

        def __call__(
            self, lower: typing.Optional[dsl.Native] = None, upper: typing.Optional[dsl.Native] = None
        ) -> dsl.Query:
            query = self.query
            if self.ordinal is not None:
                if lower:
                    query = query.where(self.ordinal >= lower)
                if upper:
                    query = query.where(self.ordinal < upper)
            elif lower or upper:
                raise forml.UnexpectedError('Bounds provided but source not ordinal')
            return query

    @classmethod
    def prepare(
        cls,
        query: dsl.Query,
        ordinal: typing.Optional[dsl.Operable],
        lower: typing.Optional[dsl.Native] = None,
        upper: typing.Optional[dsl.Native] = None,
    ) -> 'Statement':
        """Bind the particular lower/upper parameters with this prepared statement.

        Args:
            query: Base statement query.
            ordinal: Optional ordinal column specification.
            lower: Optional lower ordinal value.
            upper:  Optional upper ordinal value.

        Returns:
            prepared statement binding.
        """
        return cls(cls.Prepared(query, ordinal), lower, upper)  # pylint: disable=no-member

    def __call__(self) -> dsl.Query:
        """Expand the statement with the provided lower/upper parameters.

        Returns:
            Expanded query transformed using the associated processor.
        """
        return self.prepared(self.lower, self.upper)


class Operator(flow.Operator):
    """Basic source operator with optional label extraction.

    Label extractor is expected to be an actor with single input and two output ports - train and actual label.
    """

    def __init__(
        self, apply: flow.Spec, train: typing.Optional[flow.Spec] = None, label: typing.Optional[flow.Spec] = None
    ):
        if apply.actor.is_stateful() or (train and train.actor.is_stateful()) or (label and label.actor.is_stateful()):
            raise forml.InvalidError('Stateful actor invalid for an extractor')
        self._apply: flow.Spec = apply
        self._train: flow.Spec = train or apply
        self._label: typing.Optional[flow.Spec] = label

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Compose the source segment track.

        Returns:
            Source segment track.
        """
        if not isinstance(left, flow.Origin):
            raise forml.UnexpectedError('Source not origin')
        apply: flow.Path = flow.Path(flow.Worker(self._apply, 0, 1))
        train: flow.Path = flow.Path(flow.Worker(self._train, 0, 1))
        label: typing.Optional[flow.Path] = None
        if self._label:
            train_tail = flow.Future()
            label_tail = flow.Future()
            extract = flow.Worker(self._label, 1, 2)
            extract[0].subscribe(train.publisher)
            train_tail[0].subscribe(extract[0])
            label_tail[0].subscribe(extract[1])
            train = train.extend(tail=train_tail)
            label = train.extend(tail=label_tail)
        return flow.Trunk(apply, train, label)


class Reader(typing.Generic[parsmod.Source, parsmod.Feature, layout.Native], metaclass=abc.ABCMeta):
    """Base class for reader implementation."""

    class Actor(flow.Actor):
        """Data extraction actor using the provided reader and statement to load the data."""

        def __init__(self, reader: typing.Callable[[dsl.Query], layout.ColumnMajor], statement: Statement):
            self._reader: typing.Callable[[dsl.Query], layout.ColumnMajor] = reader
            self._statement: Statement = statement

        def __repr__(self):
            return f'{repr(self._reader)}({repr(self._statement)})'

        def apply(self) -> typing.Any:
            return self._reader(self._statement())

    def __init__(
        self,
        sources: typing.Mapping[dsl.Source, parsmod.Source],
        features: typing.Mapping[dsl.Feature, parsmod.Feature],
        **kwargs: typing.Any,
    ):
        self._sources: typing.Mapping[dsl.Source, parsmod.Source] = sources
        self._features: typing.Mapping[dsl.Feature, parsmod.Feature] = features
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __repr__(self):
        return flow.name(self.__class__, **self._kwargs)

    def __call__(self, query: dsl.Query) -> layout.ColumnMajor:
        LOGGER.debug('Parsing ETL query')
        with self.parser(self._sources, self._features) as visitor:
            query.accept(visitor)
            result = visitor.fetch()
        LOGGER.debug('Starting ETL read using: %s', result)
        return self.format(self.read(result, **self._kwargs))

    @classmethod
    @abc.abstractmethod
    def parser(
        cls,
        sources: typing.Mapping[dsl.Source, parsmod.Source],
        features: typing.Mapping[dsl.Feature, parsmod.Feature],
    ) -> parsmod.Visitor:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            features: Feature mappings to be used by the parser.

        Returns:
            Parser instance.
        """

    @classmethod
    def format(cls, data: layout.Native) -> layout.ColumnMajor:
        """Format the input data into the required payload.ColumnMajor format.

        Args:
            data: Input data.

        Returns:
            Data formatted into layout.ColumnMajor format.
        """
        return data

    @classmethod
    @abc.abstractmethod
    def read(cls, statement: parsmod.Source, **kwargs: typing.Any) -> layout.Native:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.
            kwargs: Optional reader keyword args.

        Returns:
            Data provided by the reader.
        """


class Slicer:
    """Base class for slicer implementation."""

    class Actor(flow.Actor):
        """Column extraction actor using the provided slicer to separate features from labels."""

        def __init__(
            self,
            slicer: typing.Callable[[layout.ColumnMajor, typing.Union[slice, int]], layout.ColumnMajor],
            features: typing.Sequence[dsl.Feature],
            labels: typing.Sequence[dsl.Feature],
        ):
            self._slicer: typing.Callable[[layout.ColumnMajor, typing.Union[slice, int]], layout.ColumnMajor] = slicer
            fstop = len(features)
            lcount = len(labels)
            self._features: slice = slice(fstop)
            self._label: typing.Union[slice, int] = slice(fstop, fstop + lcount) if lcount > 1 else fstop

        def apply(self, features: layout.ColumnMajor) -> tuple[typing.Any, typing.Any]:
            assert len(features) == (
                self._label.stop if isinstance(self._label, slice) else self._label + 1
            ), 'Unexpected number of features for splitting'
            return self._slicer(features, self._features), self._slicer(features, self._label)

    def __init__(self, schema: typing.Sequence[dsl.Feature], features: typing.Mapping[dsl.Feature, parsmod.Feature]):
        self._schema: typing.Sequence[dsl.Feature] = schema
        self._features: typing.Mapping[dsl.Feature, parsmod.Feature] = features

    def __call__(self, source: layout.ColumnMajor, selection: typing.Union[slice, int]) -> layout.ColumnMajor:
        LOGGER.debug('Selecting features: %s', self._schema[selection])
        return source[selection]
