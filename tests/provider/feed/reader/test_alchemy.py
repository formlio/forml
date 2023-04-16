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
import abc
import datetime
import types
import typing

import numpy
import pandas
import pytest
import sqlalchemy
from sqlalchemy import engine, sql

from forml.io import dsl
from forml.io.dsl import function
from forml.io.dsl import parser as parsmod
from forml.provider.feed.reader import alchemy


class Case(typing.NamedTuple):
    """Test case input/output."""

    query: dsl.Source
    expected: str


class Scenario(abc.ABC):
    """Base class for parser testing scenarios."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def case(student_table: dsl.Table, school_table: dsl.Table) -> Case:
        """Test case query and expected result."""

    @staticmethod
    def clean(value: str) -> str:
        """Replace all whitespace with single space."""
        return ' '.join(value.strip().split())

    @pytest.fixture(scope='session')
    def statement(self, parser: parsmod.Visitor, case: Case) -> str:
        """Statement fixture."""

        def compile(clause: sql.ClauseElement) -> str:  # pylint: disable=redefined-builtin
            """Compile the clause into a string representation."""
            return str(clause.compile(compile_kwargs={'literal_binds': True}))

        with parser:
            case.query.accept(parser)
            return self.clean(compile(parser.fetch()))

    @pytest.fixture(scope='session')
    def expected(self, case: Case) -> str:
        """Expected fixture."""
        return self.clean(case.expected)

    def test(self, statement: str, expected: str):
        """Actual test logic."""
        assert statement == expected


@pytest.fixture(scope='session')
def sources(
    student_table: dsl.Table, school_table: dsl.Table, person_table: dsl.Table
) -> typing.Mapping[dsl.Source, sql.Selectable]:
    """Sources mapping fixture."""
    sql_student = sql.table(sql.quoted_name('student', quote=True))
    return types.MappingProxyType(
        {
            student_table: sql_student,
            school_table: sql.table(sql.quoted_name('school', quote=True)),
            student_table.inner_join(person_table, student_table.surname == person_table.surname): sql_student,
            person_table: sql.table(sql.quoted_name('person', quote=True)),
        }
    )


@pytest.fixture(scope='session')
def features(student_table: dsl.Table) -> typing.Mapping[dsl.Feature, sql.ColumnElement]:
    """Columns mapping fixture."""
    return types.MappingProxyType({student_table.level: sql.column('class')})


class TestParser:
    """Alchemy parser unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def parser(
        sources: typing.Mapping[dsl.Source, sql.Selectable],
        features: typing.Mapping[dsl.Feature, sql.ColumnElement],
    ) -> alchemy.Parser:
        """Parser fixture."""
        return alchemy.Parser(sources, features)

    class TestSelect(Scenario):
        """SQL parser select unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student_table: dsl.Table, school_table: dsl.Table) -> Case:
            return Case(
                student_table.select(student_table.surname.alias('student'), student_table.score),
                'SELECT "student"."surname" AS "student", "student"."score" FROM "student"',
            )

    class TestLiteral(Scenario):
        """SQL parser literal unit test."""

        @staticmethod
        @pytest.fixture(
            scope='session',
            params=(
                Case(1, 1),
                Case('a', "'a'"),
                Case(datetime.date(2020, 7, 9), "'2020-07-09'"),
                Case(datetime.datetime(2020, 7, 9, 7, 38, 21, 123456), "'2020-07-09 07:38:21.123456'"),
            ),
        )
        def case(request, student_table: dsl.Table, school_table: dsl.Table) -> Case:
            query = student_table.select(dsl.Literal(request.param.query).alias('literal'))
            expected = f'SELECT {request.param.expected} AS "literal" FROM "student"'
            return Case(query, expected)

    class TestExpression(Scenario):
        """SQL parser expression unit test."""

        @staticmethod
        @pytest.fixture(
            scope='session',
            params=(
                Case(
                    (function.Cast(dsl.Literal('1'), dsl.Integer()) + 1).alias('int'),
                    '''CAST('1' AS INTEGER) + 1 AS "int"''',
                ),
                Case(((1 + dsl.Literal(1)) * 2).alias('int'), '''(1 + 1) * 2 AS "int"'''),
                Case(
                    function.Year(datetime.datetime(2020, 7, 9, 16, 58, 32, 654321)).alias('year'),
                    '''year('2020-07-09 16:58:32.654321') AS "year"''',
                ),
                Case(function.Year(datetime.date(2020, 7, 9)).alias('year'), '''year('2020-07-09') AS "year"'''),
                Case(
                    (2 * (function.Year(datetime.date(2020, 7, 9)) + dsl.Literal(1))).alias('calc'),
                    '''2 * (year('2020-07-09') + 1) AS "calc"''',
                ),
            ),
        )
        def case(request, student_table: dsl.Table, school_table: dsl.Table) -> Case:
            query = student_table.select(request.param.query)
            expected = f'SELECT {request.param.expected} FROM "student"'
            return Case(query, expected)

    class TestJoin(Scenario):
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
        @pytest.fixture(
            scope='session',
            params=(
                dsl.Join.Kind.LEFT,
                dsl.Join.Kind.RIGHT,
                dsl.Join.Kind.FULL,
                dsl.Join.Kind.INNER,
                dsl.Join.Kind.CROSS,
            ),
        )
        def case(cls, request, student_table: dsl.Table, school_table: dsl.Table) -> Case:
            condition, sql = cls.condition(request.param, student_table, school_table)
            query = dsl.Join(student_table, school_table, request.param, condition).select(
                student_table.surname, school_table.name
            )
            expected = f'SELECT "student"."surname", "school"."name" FROM {cls.join(request.param)}{sql}'
            return Case(query, expected)

        @classmethod
        def join(cls, kind: dsl.Join.Kind) -> str:
            """The join snippet."""
            if kind != dsl.Join.Kind.RIGHT:
                return f'"student" {cls.KIND[kind]} "school"'
            return f'"school" {cls.KIND[kind]} "student"'

        @staticmethod
        def condition(
            kind: dsl.Join.Kind, student: dsl.Table, school: dsl.Table
        ) -> tuple[typing.Optional[dsl.Expression], str]:
            """Condition helper."""
            if kind != dsl.Join.Kind.CROSS:
                return school.sid == student.school, ' ON "school"."id" = "student"."school"'
            return None, ' ON true'

    class TestOrderBy(Scenario):
        """SQL parser orderby unit test."""

        @staticmethod
        @pytest.fixture(
            scope='session',
            params=(
                Case(None, 'ASC'),
                Case(dsl.Ordering.Direction.ASCENDING, 'ASC'),
                Case(dsl.Ordering.Direction.DESCENDING, 'DESC'),
            ),
        )
        def case(request, student_table: dsl.Table, school_table: dsl.Table) -> Case:
            query = student_table.select(student_table.score).orderby(
                dsl.Ordering(student_table.score, request.param.query)
            )
            expected = f'SELECT "student"."score" FROM "student" ORDER BY "student"."score" {request.param.expected}'
            return Case(query, expected)

    class TestQuery(Scenario):
        """SQL parser unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student_table: dsl.Table, school_table: dsl.Table) -> Case:
            query = (
                student_table.inner_join(school_table, school_table.sid == student_table.school)
                .select(student_table.surname.alias('student'), function.Count(school_table.name).alias('num'))
                .groupby(student_table.surname)
                .having(function.Count(school_table.name) > 1)
                .where(student_table.score < 2)
                .orderby(student_table.level, student_table.score, 'descending')
                .limit(10)
            )
            expected = (
                'SELECT "student"."surname" AS "student", count("school"."name") AS "num" '
                'FROM "student" JOIN "school" ON "school"."id" = "student"."school" '
                'WHERE "student"."score" < 2 GROUP BY "student"."surname" HAVING count("school"."name") > 1 '
                'ORDER BY "student"."class" ASC, "student"."score" DESC '
                'LIMIT 10'
            )
            return Case(query, expected)

    class TestReference(Scenario):
        """SQL parser reference unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student_table: dsl.Table, school_table: dsl.Table) -> Case:
            student_table = student_table.reference('foo')
            subquery = (
                student_table.inner_join(school_table, school_table.sid == student_table.school)
                .select(student_table.surname.alias('student'), school_table.name.alias('school'))
                .reference('bar')
            )
            query = subquery.select(subquery.student)
            expected = (
                'SELECT "bar"."student" FROM (SELECT "foo"."surname" AS "student", "school"."name" AS "school" '
                'FROM "student" AS "foo" JOIN "school" ON "school"."id" = "foo"."school") AS "bar"'
            )
            return Case(query, expected)


class TestReader:
    """Alchemy reader test."""

    @staticmethod
    @pytest.fixture(scope='function', params=['sqlite:///:memory:', 'duckdb:///:memory:'])
    def connection(request: pytest.FixtureRequest, student_data: pandas.DataFrame, school_data: pandas.DataFrame):
        """Populated in-memory connection fixture."""
        connection = sqlalchemy.create_engine(request.param).connect()
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
        self,
        reader: alchemy.Reader,
        source_query: dsl.Query,
        student_data: pandas.DataFrame,
        school_data: pandas.DataFrame,
    ):
        """Test the read operation."""
        result = reader(source_query)
        expected = (
            student_data[student_data['score'] > 0]
            .sort_values(['updated', 'surname'])
            .set_index('school')
            .join(school_data.set_index('id'))[['surname', 'name', 'score']]
            .apply(lambda r: pandas.Series([r['surname'], r['name'], int(r['score'])]), axis='columns')
        ).values
        assert numpy.array_equal(result.to_rows(), expected)
