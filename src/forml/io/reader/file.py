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

from forml import io
from forml.io.dsl.parser import code
from forml.io.dsl.schema import series, frame
from forml.io.etl import extract

LOGGER = logging.getLogger(__name__)


class Handle(metaclass=abc.ABCMeta):
    """Abstract file handle.
    """
    @property
    @abc.abstractmethod
    def header(self) -> typing.Sequence[str]:
        """Get the column names.

        Returns: Column names.
        """

    @abc.abstractmethod
    def read(self, columns: typing.Sequence[str],
             predicate: typing.Optional[series.Expression] = None) -> extract.Columnar:
        """Read the file columns.

        Args:
            columns:
            predicate:

        Returns:
        """


class Set(typing.NamedTuple):
    train: Handle
    apply: Handle

    def read(self, columns: typing.Sequence[str],
             predicate: typing.Optional[series.Expression] = None) -> extract.Columnar:
        if set(self.apply.header).issuperset(columns):
            source = self.apply
        elif not set(self.train.header).issuperset(columns):
            raise RuntimeError('Unknown columns')
        else:
            source = self.train
        return source.read(columns, predicate)


class Feed(io.Feed[code.Tabulizer, code.Columnizer]):
    """Abstract file based feed.
    """
    class Reader(extract.Reader[code.Tabulizer, code.Columnizer, code.Table], metaclass=abc.ABCMeta):
        """File based reader.
        """
        class Parser(code.Frame, metaclass=abc.ABCMeta):
            """Parser producing code that implements the actual ETL.
            """

        @classmethod
        def format(cls, data: code.Table) -> extract.Columnar:
            return super().format(data)

        @classmethod
        def read(cls, statement: code.Tabulizer, **kwargs) -> code.Table:
            return statement(None)

        @classmethod
        def parser(cls, sources: typing.Mapping[frame.Source, code.Tabulizer],
                   columns: typing.Mapping[series.Column, code.Columnizer]) -> 'Feed.Reader.Parser':
            return cls.Parser(columns, sources)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def reader(cls, sources: typing.Mapping[frame.Source, code.Tabulizer],
               columns: typing.Mapping[series.Column, code.Columnizer],
               **kwargs) -> typing.Callable[[frame.Query], extract.Columnar]:
        def read(query: frame.Query) -> typing.Any:
            """Reader callback.

            Args:
                query: Input query instance.

            Returns: Data.
            """
            fields = series.Element.dissect(*query.columns)
            # assertion parser to enforce: single source, no expressions, no aggregations, ...
            assert len({f.table for f in fields}) == 1, 'File supports only single source'

            # return sources[].train if labels in fields else sources[].test

        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated