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
ANSI SQL ETL feed.
"""
import abc
import contextlib
import logging
import typing

from forml import io
from forml.io.dsl.parser import sql as sqlmod
from forml.io.dsl.schema import series, frame
from forml.io.etl import extract
from forml.io.etl.extract import Columnar

LOGGER = logging.getLogger(__name__)


class Feed(io.Feed[str]):
    """SQL feed with abstract reader.
    """
    class Reader(extract.Reader[str], metaclass=abc.ABCMeta):
        """SQL reader base class for PEP249 compliant DB APIs.
        """
        class Parser(sqlmod.Parser):
            """Custom parser implementation.
            """

        @classmethod
        @abc.abstractmethod
        def connection(cls, **kwargs: typing.Any):
            """Create a PEP249 compliant connection instance.

            Args:
                **kwargs: Connection specific keyword arguments.

            Returns: Connection instance.
            """

        @classmethod
        def parser(cls, sources: typing.Mapping[frame.Source, str],
                   columns: typing.Mapping[series.Column, str]) -> 'Feed.Reader.Parser':
            """Return the parser instance of this reader.

            Args:
                sources: Source mappings to be used by the parser.
                columns: Column mappings to be used by the parser.

            Returns: Parser instance.
            """
            return cls.Parser(sources, columns)

        @classmethod
        def format(cls, data: typing.Sequence[typing.Sequence[typing.Any]]) -> Columnar:
            """PEP249 assumes row oriented results, we need columnar so let's transpose here.

            Args:
                data: Row oriented input.

            Returns: Columnar output.
            """
            return extract.transpose(data)

        @classmethod
        def read(cls, statement: str, **kwargs) -> typing.Sequence[typing.Sequence[typing.Any]]:
            """Perform the read operation with the given statement.

            Args:
                statement: Query statement in the reader's native syntax.
                kwargs: Optional reader keyword args.

            Returns: Row-oriented data provided by the reader.
            """
            LOGGER.debug('Establishing SQL connection')
            with contextlib.closing(cls.connection(**kwargs)) as connection:
                cursor = connection.cursor()
                LOGGER.debug('Executing SQL query')
                cursor.execute(statement)
                return cursor.fetchall()

    @classmethod
    def reader(cls, sources: typing.Mapping[frame.Source, str], columns: typing.Mapping[series.Column, str],
               **kwargs: typing.Any) -> typing.Callable[[frame.Query], extract.Columnar]:
        """Return the reader instance of this feed.

        Args:
            sources: Source mappings to be used by the reader.
            columns: Column mappings to be used by the reader.
            kwargs: Optional reader keyword arguments.

        Returns: Reader instance.
        """
        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated
