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
File based ETL feed.
"""
import abc
import logging
import typing

from forml.io import payload, feed
from forml.io.dsl.parser import code
from forml.io.dsl.struct import series, frame
from forml.io.feed import extract

LOGGER = logging.getLogger(__name__)


class Handle(metaclass=abc.ABCMeta):
    """Abstract file handle."""

    @property
    @abc.abstractmethod
    def header(self) -> typing.Sequence[str]:
        """Get the column names.

        Returns:
            Column names.
        """

    @abc.abstractmethod
    def read(
        self, columns: typing.Sequence[str], predicate: typing.Optional[series.Expression] = None
    ) -> payload.ColumnMajor:
        """Read the file columns.

        Args:
            columns:
            predicate:

        Returns:
        """


class Set(typing.NamedTuple):
    """File based train/apply set."""

    train: Handle
    apply: Handle

    def read(
        self, columns: typing.Sequence[str], predicate: typing.Optional[series.Expression] = None
    ) -> payload.ColumnMajor:
        """Read the dataset."""
        if set(self.apply.header).issuperset(columns):
            source = self.apply
        elif not set(self.train.header).issuperset(columns):
            raise RuntimeError('Unknown columns')
        else:
            source = self.train
        return source.read(columns, predicate)


class Feed(feed.Provider[code.Tabulizer, code.Columnizer]):
    """Abstract file based feed."""

    class Reader(extract.Reader[code.Tabulizer, code.Columnizer, code.Table], metaclass=abc.ABCMeta):
        """File based reader."""

        class Parser(code.Frame, metaclass=abc.ABCMeta):
            """Parser producing code that implements the actual ETL."""

        @classmethod
        def format(cls, data: code.Table) -> payload.ColumnMajor:
            """Format the input data into the required payload.Columnar format.

            Args:
                data: Input data.

            Returns:
                Data formatted into payload.Columnar format.
            """
            return super().format(data)

        @classmethod
        def read(cls, statement: code.Tabulizer, **_) -> code.Table:
            """Perform the read operation with the given statement.

            Args:
                statement: Query statement in the reader's native syntax.
                kwargs: Optional reader keyword args.

            Returns:
                Data provided by the reader.
            """
            return statement(None)

        @classmethod
        def parser(
            cls,
            sources: typing.Mapping[frame.Source, code.Tabulizer],
            columns: typing.Mapping[series.Column, code.Columnizer],
        ) -> 'Feed.Reader.Parser':
            """Return the parser instance of this reader.

            Args:
                sources: Source mappings to be used by the parser.
                columns: Column mappings to be used by the parser.

            Returns:
                Parser instance.
            """
            return cls.Parser(columns, sources)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def reader(
        cls,
        sources: typing.Mapping[frame.Source, code.Tabulizer],
        columns: typing.Mapping[series.Column, code.Columnizer],
        **kwargs,
    ) -> typing.Callable[[frame.Query], payload.ColumnMajor]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.
            kwargs: Optional reader keyword arguments.

        Returns:
            Reader instance.
        """
        # def read(query: frame.Query) -> typing.Any:
        #     """Reader callback.
        #
        #     Args:
        #         query: Input query instance.
        #
        #     Returns:
        #         Data.
        #     """
        #     fields = series.Element.dissect(*query.columns)
        #     # assertion parser to enforce: single source, no expressions, no aggregations, ...
        #     assert len({f.table for f in fields}) == 1, 'File supports only single source'
        #
        #     # return sources[].train if labels in fields else sources[].test

        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated
