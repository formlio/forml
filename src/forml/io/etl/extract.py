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

"""Extract utilities.
"""
import abc
import logging
import typing

from forml import error
from forml.flow import task, pipeline
from forml.flow.graph import node, view
from forml.flow.pipeline import topology
from forml.io.dsl import parser as parsmod
from forml.io.dsl.schema import kind as kindmod, series, frame

LOGGER = logging.getLogger(__name__)


class Statement(typing.NamedTuple):
    """Select statement defined as a query and definition of the ordinal expression.
    """
    prepared: 'Prepared'
    lower: typing.Optional[kindmod.Native]
    upper: typing.Optional[kindmod.Native]

    class Prepared(typing.NamedTuple):
        """Statement bound with particular lower/upper parameters.
        """
        query: frame.Query
        ordinal: typing.Optional[series.Element]

        def __call__(self, lower: typing.Optional[kindmod.Native] = None,
                     upper: typing.Optional[kindmod.Native] = None) -> frame.Query:
            query = self.query
            if self.ordinal is not None:
                if lower:
                    query = query.where(self.ordinal >= lower)
                if upper:
                    query = query.where(self.ordinal < upper)
            elif lower or upper:
                raise error.Unexpected('Bounds provided but source not ordinal')
            return query

    @classmethod
    def prepare(cls, query: frame.Query, ordinal: typing.Optional[series.Element],
                lower: typing.Optional[kindmod.Native] = None,
                upper: typing.Optional[kindmod.Native] = None) -> 'Statement':
        """Bind the particular lower/upper parameters with this prepared statement.

        Args:
            query: Base statement query.
            ordinal: Optional ordinal column specification.
            lower: Optional lower ordinal value.
            upper:  Optional upper ordinal value.

        Returns: prepared statement binding.
        """
        return cls(cls.Prepared(query, ordinal), lower, upper)  # pylint: disable=no-member

    def __call__(self) -> frame.Query:
        """Expand the statement with the provided lower/upper parameters.

        Returns: Expanded query transformed using the associated processor.
        """
        return self.prepared(self.lower, self.upper)


class Operator(topology.Operator):
    """Basic source operator with optional label extraction.

    Label extractor is expected to be an actor with single input and two output ports - train and actual label.
    """
    def __init__(self, apply: task.Spec, train: typing.Optional[task.Spec] = None,
                 label: typing.Optional[task.Spec] = None):
        self._apply: task.Spec = apply
        self._train: task.Spec = train or apply
        self._label: typing.Optional[task.Spec] = label

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Compose the source segment track.

        Returns: Source segment track.
        """
        if not isinstance(left, topology.Origin):
            raise error.Invalid('Source not origin')
        apply: view.Path = view.Path(node.Worker(self._apply, 0, 1))
        train: view.Path = view.Path(node.Worker(self._train, 0, 1))
        label: typing.Optional[view.Path] = None
        if self._label:
            train_tail = node.Future()
            label_tail = node.Future()
            extract = node.Worker(self._label, 1, 2)
            extract[0].subscribe(train.publisher)
            train_tail[0].subscribe(extract[0])
            label_tail[0].subscribe(extract[1])
            train = train.extend(tail=train_tail)
            label = train.extend(tail=label_tail)
        return pipeline.Segment(apply, train, label)


def transpose(data: typing.Sequence[typing.Sequence[typing.Any]]) -> typing.Sequence[typing.Iterator[typing.Any]]:
    """Helper for transposing between row and column oriented matrices. The output columns are from performance reason
    just lazy generators.

    Args:
        data: Input matrix.

    Returns: Transposed output matrix.
    """
    def col(idx: int) -> typing.Iterator[typing.Any]:
        """Create a generator for given column index.

        Args:
            idx: Index of column to be generated.

        Returns: Generator for given column.
        """
        return (data[r][idx] for r in range(nrows))

    if data:
        nrows = len(data)
        ncols = len(data[0])
        data = [col(c) for c in range(ncols)]
    return data


Columnar = typing.Sequence[typing.Any]  # Sequence of columns of any type
Native = typing.TypeVar('Native')


class Reader(typing.Generic[parsmod.Source, parsmod.Column, Native], metaclass=abc.ABCMeta):
    """Base class for reader implementation.
    """
    class Actor(task.Actor):
        """Data extraction actor using the provided reader and statement to load the data.
        """
        def __init__(self, reader: typing.Callable[[frame.Query], Columnar], statement: Statement):
            self._reader: typing.Callable[[frame.Query], Columnar] = reader
            self._statement: Statement = statement

        def apply(self) -> typing.Any:
            return self._reader(self._statement())

    def __init__(self, sources: typing.Mapping[frame.Source, parsmod.Source],
                 columns: typing.Mapping[series.Column, parsmod.Column],
                 **kwargs: typing.Any):
        self._sources: typing.Mapping[frame.Source, parsmod.Source] = sources
        self._columns: typing.Mapping[series.Column, parsmod.Column] = columns
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __call__(self, query: frame.Query) -> Columnar:
        LOGGER.debug('Parsing ETL query')
        with self.parser(self._sources, self._columns) as visitor:
            query.accept(visitor)
            result = visitor.pop()
        LOGGER.debug('Starting ETL read using: %s', result)
        return self.format(self.read(result, **self._kwargs))

    @classmethod
    @abc.abstractmethod
    def parser(cls, sources: typing.Mapping[frame.Source, parsmod.Source],
               columns: typing.Mapping[series.Column, parsmod.Column]) -> parsmod.Frame:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            columns: Column mappings to be used by the parser.

        Returns: Parser instance.
        """

    @classmethod
    def format(cls, data: Native) -> Columnar:
        """Format the input data into the required Columnar format.

        Args:
            data: Input data.

        Returns: Data formatted into Columnar format.
        """
        return data

    @classmethod
    @abc.abstractmethod
    def read(cls, statement: parsmod.Source, **kwargs: typing.Any) -> Native:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.
            kwargs: Optional reader keyword args.

        Returns: Data provided by the reader.
        """


class Slicer:
    """Base class for slicer implementation.
    """
    class Actor(task.Actor):
        """Column extraction actor using the provided slicer to separate features from labels.
        """
        def __init__(self, slicer: typing.Callable[[Columnar, typing.Union[slice, int]], Columnar],
                     features: typing.Sequence[series.Column], labels: typing.Sequence[series.Column]):
            self._slicer: typing.Callable[[Columnar, typing.Union[slice, int]], Columnar] = slicer
            fstop = len(features)
            lcount = len(labels)
            self._features: slice = slice(fstop)
            self._label: typing.Union[slice, int] = slice(fstop, fstop + lcount) if lcount > 1 else fstop

        def apply(self, columns: Columnar) -> typing.Tuple[typing.Any, typing.Any]:
            assert len(columns) == (self._label.stop if isinstance(self._label, slice)
                                    else self._label + 1), 'Unexpected number of columns for splitting'
            return self._slicer(columns, self._features), self._slicer(columns, self._label)

    def __init__(self, schema: typing.Sequence[series.Column], columns: typing.Mapping[series.Column, parsmod.Column]):
        self._schema: typing.Sequence[series.Column] = schema
        self._columns: typing.Mapping[series.Column, parsmod.Column] = columns

    def __call__(self, source: Columnar, selection: typing.Union[slice, int]) -> Columnar:
        LOGGER.debug('Selecting columns: %s', self._schema[selection])
        return source[selection]
