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
SQL alchemy parser tests.
"""
# pylint: disable=no-self-use

import types
import typing

import numpy
import pandas
import pytest
import sqlalchemy
from pyhive import sqlalchemy_trino as trino
from sqlalchemy import engine, sql

from forml.io import dsl
from forml.io.dsl import parser as parsmod
from forml.lib.feed.reader.sql import alchemy

from . import Parser


@pytest.fixture(scope='session')
def sources(student: dsl.Table, school: dsl.Table, person: dsl.Table) -> typing.Mapping[dsl.Source, sql.Selectable]:
    """Sources mapping fixture."""
    student_table = sql.table('student')
    return types.MappingProxyType(
        {
            student: student_table,
            school: sql.table('school'),
            dsl.Join(student, person, student.surname == person.surname): student_table,
            person: sql.table('person'),
        }
    )


@pytest.fixture(scope='session')
def features(student: dsl.Table) -> typing.Mapping[dsl.Feature, sql.ColumnElement]:
    """Columns mapping fixture."""
    return types.MappingProxyType({student.level: sql.column('class')})


class TestParser(Parser):
    """SQL parser unit tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    def parser(
        sources: typing.Mapping[dsl.Source, sql.Selectable],
        features: typing.Mapping[dsl.Feature, sql.ColumnElement],
    ) -> alchemy.Parser:
        """Parser fixture."""
        return alchemy.Parser(sources, features)

    @staticmethod
    @pytest.fixture(scope='session')
    def formatter() -> typing.Callable[[parsmod.Source], str]:
        return lambda s: str(s.compile(dialect=trino.TrinoDialect(), compile_kwargs={'literal_binds': True}))

    class TestJoin(Parser.TestJoin):
        """SQL parser join unit test."""

        KIND = {
            None: 'JOIN',
            dsl.Join.Kind.LEFT: 'LEFT OUTER JOIN',
            dsl.Join.Kind.RIGHT: 'LEFT OUTER JOIN',
            dsl.Join.Kind.FULL: 'FULL OUTER JOIN',
            dsl.Join.Kind.INNER: 'JOIN',
            dsl.Join.Kind.CROSS: 'FULL OUTER JOIN',
        }

        @classmethod
        def join(cls, kind: dsl.Join.Kind) -> str:
            if kind != dsl.Join.Kind.RIGHT:
                return f'"student" {cls.KIND[kind]} "school"'
            return f'"school" {cls.KIND[kind]} "student"'

        @staticmethod
        def condition(
            kind: dsl.Join.Kind, student: dsl.Table, school: dsl.Table
        ) -> tuple[typing.Optional[dsl.Expression], str]:
            if kind != dsl.Join.Kind.CROSS:
                return school.sid == student.school, ' ON "school"."id" = "student"."school"'
            return None, ' ON true'


class TestReader:
    """Alchemy reader test."""

    @staticmethod
    @pytest.fixture(scope='function')
    def connection(student_data: pandas.DataFrame, school_data: pandas.DataFrame):
        """Populated in-memory SQLite connection fixture."""
        connection = sqlalchemy.create_engine('sqlite:///:memory:').connect()
        student_data.rename({'level': 'class'}, axis='columns').to_sql('student', connection, index=False)
        school_data.to_sql('school', connection, index=False)
        return connection

    @staticmethod
    @pytest.fixture(scope='function')
    def reader(
        connection: engine.Connectable,
        sources: typing.Mapping[dsl.Source, sql.Selectable],
        features: typing.Mapping[dsl.Feature, sql.ColumnElement],
    ) -> alchemy.Reader:
        """Aclhemy reader fixture."""
        return alchemy.Reader(sources, features, connection)

    def test_read(
        self, reader: alchemy.Reader, query: dsl.Query, student_data: pandas.DataFrame, school_data: pandas.DataFrame
    ):
        """Test the read operation."""
        result = reader(query)
        expected = (
            student_data[student_data['score'] < 2]
            .sort_values(['level', 'score'])
            .set_index('school')
            .join(school_data.set_index('id'))[['surname', 'name', 'score']]
            .apply(lambda s: s.apply(str))
        ).values.transpose()
        assert numpy.array_equal(result, expected)
