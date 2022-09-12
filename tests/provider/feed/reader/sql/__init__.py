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
Common SQL parser tests.
"""
import abc
import datetime
import typing

import pytest

from forml.io import dsl
from forml.io.dsl import function
from forml.io.dsl import parser as parsmod


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

    @pytest.fixture(scope='session')
    def statement(
        self,
        parser: parsmod.Visitor,
        case: Case,
        formatter: typing.Callable[[parsmod.Source], str],
        cleaner: typing.Callable[[str], str],
    ) -> str:
        """Statement fixture."""
        with parser:
            case.query.accept(parser)
            return cleaner(formatter(parser.fetch()))

    @pytest.fixture(scope='session')
    def expected(self, case: Case, cleaner: typing.Callable[[str], str]) -> str:
        """Expected fixture."""
        return cleaner(case.expected)

    def test(self, statement: str, expected: str):
        """Actual test logic."""
        assert statement == expected


class Parser(metaclass=abc.ABCMeta):
    """SQL parser unit tests base class."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def parser(sources: typing.Mapping[dsl.Source, str], features: typing.Mapping[dsl.Feature, str]) -> parsmod.Visitor:
        """Parser fixture."""

    @staticmethod
    @pytest.fixture(scope='session')
    def cleaner() -> typing.Callable[[str], str]:
        """Fixture providing the processor for cleaning the statement text."""

        def strip(value: str) -> str:
            """Replace all whitespace with single space."""
            return ' '.join(value.strip().split())

        return strip

    @staticmethod
    @pytest.fixture(scope='session')
    def formatter() -> typing.Callable[[parsmod.Source], str]:
        """Fixture providing the processor for formatting/encoding the compiled result."""
        return str

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
                Case(datetime.date(2020, 7, 9), "DATE '2020-07-09'"),
                Case(datetime.datetime(2020, 7, 9, 7, 38, 21, 123456), "TIMESTAMP '2020-07-09 07:38:21.123456'"),
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
                    '''year(TIMESTAMP '2020-07-09 16:58:32.654321') AS "year"''',
                ),
                Case(function.Year(datetime.date(2020, 7, 9)).alias('year'), '''year(DATE '2020-07-09') AS "year"'''),
                Case(
                    (2 * (function.Year(datetime.date(2020, 7, 9)) + dsl.Literal(1))).alias('calc'),
                    '''2 * (year(DATE '2020-07-09') + 1) AS "calc"''',
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
            dsl.Join.Kind.RIGHT: 'RIGHT OUTER JOIN',
            dsl.Join.Kind.FULL: 'FULL OUTER JOIN',
            dsl.Join.Kind.INNER: 'JOIN',
            dsl.Join.Kind.CROSS: 'CROSS JOIN',
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
            return f'"student" {cls.KIND[kind]} "school"'

        @staticmethod
        def condition(
            kind: dsl.Join.Kind, student: dsl.Table, school: dsl.Table
        ) -> tuple[typing.Optional[dsl.Expression], str]:
            """The condition snippet."""
            if kind != dsl.Join.Kind.CROSS:
                return school.sid == student.school, ' ON "school"."id" = "student"."school"'
            return None, ''

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
