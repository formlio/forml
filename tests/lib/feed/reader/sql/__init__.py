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
# pylint: disable=no-self-use
import abc
import datetime
import typing

import pytest

from forml.io.dsl import function, parser as parsmod
from forml.io.dsl.struct import series, frame, kind


class Case(typing.NamedTuple):
    """Test case input/output."""

    query: frame.Source
    expected: str


class Scenario(abc.ABC):
    """Base class for parser testing scenarios."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def case(student: frame.Table, school: frame.Table) -> Case:
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
    def parser(
        sources: typing.Mapping[frame.Source, str], columns: typing.Mapping[series.Column, str]
    ) -> parsmod.Visitor:
        """Parser fixture."""

    @staticmethod
    @pytest.fixture(scope='session')
    def cleaner() -> typing.Callable[[str], str]:
        """Fixture providing the processor for cleaning the statement20 text."""

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
        def case(student: frame.Table, school: frame.Table) -> Case:
            return Case(
                student.select(student.surname.alias('student'), student.score),
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
        def case(request, student: frame.Table, school: frame.Table) -> Case:
            query = student.select(series.Literal(request.param.query).alias('literal'))
            expected = f'SELECT {request.param.expected} AS "literal" FROM "student"'
            return Case(query, expected)

    class TestExpression(Scenario):
        """SQL parser expression unit test."""

        @staticmethod
        @pytest.fixture(
            scope='session',
            params=(
                Case(
                    (function.Cast(series.Literal('1'), kind.Integer()) + 1).alias('int'),
                    '''CAST('1' AS INTEGER) + 1 AS "int"''',
                ),
                Case(((1 + series.Literal(1)) * 2).alias('int'), '''(1 + 1) * 2 AS "int"'''),
                Case(
                    function.Year(datetime.datetime(2020, 7, 9, 16, 58, 32, 654321)).alias('year'),
                    '''year(TIMESTAMP '2020-07-09 16:58:32.654321') AS "year"''',
                ),
                Case(function.Year(datetime.date(2020, 7, 9)).alias('year'), '''year(DATE '2020-07-09') AS "year"'''),
                Case(
                    (2 * (function.Year(datetime.date(2020, 7, 9)) + series.Literal(1))).alias('calc'),
                    '''2 * (year(DATE '2020-07-09') + 1) AS "calc"''',
                ),
            ),
        )
        def case(request, student: frame.Table, school: frame.Table) -> Case:
            query = student.select(request.param.query)
            expected = f'SELECT {request.param.expected} FROM "student"'
            return Case(query, expected)

    class TestJoin(Scenario):
        """SQL parser join unit test."""

        KIND = {
            None: 'JOIN',
            frame.Join.Kind.LEFT: 'LEFT OUTER JOIN',
            frame.Join.Kind.RIGHT: 'RIGHT OUTER JOIN',
            frame.Join.Kind.FULL: 'FULL OUTER JOIN',
            frame.Join.Kind.INNER: 'JOIN',
            frame.Join.Kind.CROSS: 'CROSS JOIN',
        }

        @classmethod
        @pytest.fixture(
            scope='session',
            params=(
                None,
                frame.Join.Kind.LEFT,
                frame.Join.Kind.RIGHT,
                frame.Join.Kind.FULL,
                frame.Join.Kind.INNER,
                frame.Join.Kind.CROSS,
            ),
        )
        def case(cls, request, student: frame.Table, school: frame.Table) -> Case:
            dsl, sql = cls.condition(request.param, student, school)
            query = student.join(school, dsl, kind=request.param).select(student.surname, school.name)
            expected = f'SELECT "student"."surname", "school"."name" FROM {cls.join(request.param)}{sql}'
            return Case(query, expected)

        @classmethod
        def join(cls, kind: frame.Join.Kind) -> str:
            """The join snippet."""
            return f'"student" {cls.KIND[kind]} "school"'

        @staticmethod
        def condition(
            kind: frame.Join.Kind, student: frame.Table, school: frame.Table
        ) -> tuple[typing.Optional[series.Expression], str]:
            """The condition snippet."""
            if kind != frame.Join.Kind.CROSS:
                return school.sid == student.school, ' ON "school"."id" = "student"."school"'
            return None, ''

    class TestOrderBy(Scenario):
        """SQL parser orderby unit test."""

        @staticmethod
        @pytest.fixture(
            scope='session',
            params=(
                Case(None, 'ASC'),
                Case(series.Ordering.Direction.ASCENDING, 'ASC'),
                Case(series.Ordering.Direction.DESCENDING, 'DESC'),
            ),
        )
        def case(request, student: frame.Table, school: frame.Table) -> Case:
            query = student.select(student.score).orderby(series.Ordering(student.score, request.param.query))
            expected = f'SELECT "student"."score" FROM "student" ORDER BY "student"."score" {request.param.expected}'
            return Case(query, expected)

    class TestQuery(Scenario):
        """SQL parser unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student: frame.Table, school: frame.Table) -> Case:
            query = (
                student.join(school, school.sid == student.school)
                .select(student.surname.alias('student'), function.Count(school.name).alias('num'))
                .groupby(student.surname)
                .having(function.Count(school.name) > 1)
                .where(student.score < 2)
                .orderby(student.level, student.score, 'descending')
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
        def case(student: frame.Table, school: frame.Table) -> Case:
            student = student.reference('foo')
            subquery = (
                student.join(school, school.sid == student.school)
                .select(student.surname.alias('student'), school.name.alias('school'))
                .reference('bar')
            )
            query = subquery.select(subquery.student)
            expected = (
                'SELECT "bar"."student" FROM (SELECT "foo"."surname" AS "student", "school"."name" AS "school" '
                'FROM "student" AS "foo" JOIN "school" ON "school"."id" = "foo"."school") AS "bar"'
            )
            return Case(query, expected)
