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
            Prepared statement binding.
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
        self,
        apply: flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, layout.RowMajor]],
        train: flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, layout.Tabular]],
        label: typing.Optional[
            flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, tuple[layout.RowMajor, layout.RowMajor]]]
        ] = None,
    ):
        if apply.actor.is_stateful() or (train and train.actor.is_stateful()) or (label and label.actor.is_stateful()):
            raise forml.InvalidError('Stateful actor invalid for an extractor')
        self._apply: flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, layout.RowMajor]] = apply
        self._train: flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, layout.Tabular]] = train
        self._label: typing.Optional[
            flow.Spec[flow.Actor[typing.Optional[layout.Entry], None, tuple[layout.RowMajor, layout.RowMajor]]]
        ] = label

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


Producer = typing.Callable[[dsl.Query, typing.Optional[layout.Entry]], layout.Tabular]
Output = typing.TypeVar('Output')


class Driver(typing.Generic[Output], flow.Actor[typing.Optional[layout.Entry], None, Output], metaclass=abc.ABCMeta):
    """Data extraction actor using the provided reader and statement to load the data."""

    def __init__(self, producer: Producer, statement: Statement):
        self._producer: Producer = producer
        self._statement: Statement = statement

    def __repr__(self):
        return f'{repr(self._producer)}({repr(self._statement)})'

    def _read(self, entry: typing.Optional[layout.Entry]) -> layout.Tabular:
        """Read handler.

        Args:
            entry: Producer entry.

        Returns:
            Tabular dataset.
        """
        return self._producer(self._statement(), entry)


class TableDriver(Driver[layout.Tabular]):
    """Actor that returns the data in the layout.Tabular format."""

    def apply(self, entry: typing.Optional[layout.Entry] = None) -> layout.Tabular:
        return self._read(entry)


class RowDriver(Driver[layout.RowMajor]):
    """Specialized version of the actor that returns the data already converted to layout.RowMajor format."""

    def apply(self, entry: typing.Optional[layout.Entry] = None) -> layout.RowMajor:
        return self._read(entry).to_rows()


class Slicer(flow.Actor[layout.Tabular, None, tuple[layout.RowMajor, layout.RowMajor]]):
    """Positional column extraction."""

    def __init__(
        self,
        features: typing.Sequence[int],
        labels: typing.Union[typing.Sequence[int], int],
    ):
        def from_vector(dataset: layout.Tabular) -> layout.RowMajor:
            return dataset.take_columns(labels).to_rows()

        def from_scalar(dataset: layout.Tabular) -> layout.RowMajor:
            return dataset.to_columns()[labels]

        self._features: typing.Sequence[int] = features
        self._labels: typing.Callable[[layout.Tabular], layout.RowMajor] = (
            from_vector if isinstance(labels, typing.Sequence) else from_scalar
        )

    def apply(self, dataset: layout.Tabular) -> tuple[layout.RowMajor, layout.RowMajor]:
        return dataset.take_columns(self._features).to_rows(), self._labels(dataset)

    @classmethod
    def from_columns(
        cls, features: typing.Sequence[dsl.Feature], labels: typing.Union[dsl.Feature, typing.Sequence[dsl.Feature]]
    ) -> tuple[typing.Sequence[dsl.Feature], 'flow.Spec[Slicer]']:
        """Helper method for creating the slicer and the combined set of columns.

        Args:
            features: Sequence of feature columns.
            labels: Single label column or sequence of label columns.

        Returns:
            Sequence of combined feature+label columns and the Slicer actor instance.
        """
        fstop = len(features)
        if isinstance(labels, dsl.Feature):
            lslice = fstop
            lseq = [labels]
        else:
            assert isinstance(labels, typing.Sequence), 'Expecting a sequence of DSL features.'
            lslice = range(fstop, fstop + len(labels))
            lseq = labels
        return (*features, *lseq), cls.spec(range(fstop), lslice)
