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
SQL parser tests.
"""
# pylint: disable=no-self-use
import abc
import datetime
import types
import typing

import pytest

from forml.io.dsl import function
from forml.io.dsl.parser import sql
from forml.io.dsl.schema import series, frame, kind


class Parser(metaclass=abc.ABCMeta):
    """SQL parser unit tests base class.
    """
    class Case(typing.NamedTuple):
        """Test case input/output.
        """
        query: frame.Query
        expected: str

        def __call__(self, parser: sql.Parser):
            def strip(value: str) -> str:
                """Replace all whitespace with single space.
                """
                return ' '.join(value.strip().split())

            with parser as visitor:
                self.query.accept(visitor)
                result = visitor.pop()
            assert strip(result) == strip(self.expected)

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(student: frame.Table, school: frame.Table) -> typing.Mapping[frame.Source, str]:
        """Sources mapping fixture.
        """
        return types.MappingProxyType({
            student: 'edu.student',
            school: 'edu.school'
        })

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(sources: typing.Mapping[frame.Source, str]) -> sql.Parser:
        """Parser fixture.
        """
        return sql.Parser(sources)

    @classmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Case:
        """Case fixture.
        """

    def test_parse(self, parser: sql.Parser, case: Case):
        """Parsing test.
        """
        case(parser)


class TestSelect(Parser):
    """SQL parser select unit test.
    """
    @classmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.select(student.surname.alias('student'), student.score)
        expected = 'SELECT edu.student.surname AS student, edu.student.score FROM edu.student'
        return cls.Case(query, expected)


class TestLiteral(Parser):
    """SQL parser literal unit test.
    """
    @classmethod
    @pytest.fixture(scope='session', params=(Parser.Case(1, 1),
                                             Parser.Case('a', "'a'"),
                                             Parser.Case(datetime.date(2020, 7, 9), "DATE '2020-07-09'"),
                                             Parser.Case(datetime.datetime(2020, 7, 9, 7, 38, 21, 123456),
                                                         "TIMESTAMP '2020-07-09 07:38:21.123456'")))
    def case(cls, request, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.select(student.score + request.param.query)
        expected = f'SELECT edu.student.score + {request.param.expected} FROM edu.student'
        return cls.Case(query, expected)


class TestExpression(Parser):
    """SQL parser expression unit test.
    """
    @classmethod
    @pytest.fixture(scope='session', params=(Parser.Case(function.Cast(series.Literal(1), kind.String()) + 1,
                                                         'cast(1 AS VARCHAR) + 1'),
                                             Parser.Case(1 + series.Literal(1) * 2, '1 + (1 * 2)'),
                                             Parser.Case(series.Literal(1) + datetime.datetime(2020, 7, 9, 16, 58, 32),
                                                         "1 + TIMESTAMP '2020-07-09 16:58:32.000000'"),
                                             Parser.Case(series.Literal(1) + datetime.date(2020, 7, 9),
                                                         "1 + DATE '2020-07-09'"),
                                             Parser.Case(2 * (datetime.date(2020, 7, 9) + series.Literal(1)),
                                                         "2 * (DATE '2020-07-09' + 1)")
                                             ))
    def case(cls, request, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.select(request.param.query)
        expected = f'SELECT {request.param.expected} FROM edu.student'
        return cls.Case(query, expected)


class TestJoin(Parser):
    """SQL parser join unit test.
    """
    class Join(typing.NamedTuple):
        """Helper for test case parameters.
        """
        kind: typing.Optional[frame.Join.Kind]
        condition: bool

    @classmethod
    @pytest.fixture(scope='session', params=(Parser.Case(Join(None, True), 'INNER'),
                                             Parser.Case(Join(frame.Join.Kind.LEFT, True), 'LEFT'),
                                             Parser.Case(Join(frame.Join.Kind.RIGHT, True), 'RIGHT'),
                                             Parser.Case(Join(frame.Join.Kind.FULL, True), 'FULL'),
                                             Parser.Case(Join(frame.Join.Kind.INNER, True), 'INNER'),
                                             Parser.Case(Join(frame.Join.Kind.CROSS, False), 'CROSS')))
    def case(cls, request, student: frame.Table, school: frame.Table) -> Parser.Case:
        if request.param.query.condition:
            dsl = student.school == school.sid
            sql = ' ON edu.student.school = edu.school.id'
        else:
            dsl = None
            sql = ''
        query = student.join(school, dsl, kind=request.param.query.kind).select(student.surname, school.name)
        expected = 'SELECT edu.student.surname, edu.school.name ' \
                   f'FROM edu.student {request.param.expected} JOIN edu.school{sql}'
        return cls.Case(query, expected)


class TestOrderBy(Parser):
    """SQL parser orderby unit test.
    """
    @classmethod
    @pytest.fixture(scope='session', params=(Parser.Case(None, 'ASC'),
                                             Parser.Case(series.Ordering.Direction.ASCENDING, 'ASC'),
                                             Parser.Case(series.Ordering.Direction.DESCENDING, 'DESC')))
    def case(cls, request, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.select(student.score).orderby(series.Ordering(student.score, request.param.query))
        expected = f'SELECT edu.student.score FROM edu.student ORDER BY edu.student.score {request.param.expected}'
        return cls.Case(query, expected)


class TestQuery(Parser):
    """SQL parser unit test.
    """
    @classmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.join(school, student.school == school.sid)\
            .select(student.surname.alias('student'), function.Count(school.name))\
            .groupby(student.surname).having(function.Count(school.name) > 1)\
            .where(student.score < 2).orderby(student.level, student.score, 'descending').limit(10)
        expected = 'SELECT edu.student.surname AS student, count(edu.school.name) ' \
                   'FROM edu.student INNER JOIN edu.school ON edu.student.school = edu.school.id ' \
                   'WHERE edu.student.score < 2 GROUP BY edu.student.surname HAVING count(edu.school.name) > 1 ' \
                   'ORDER BY edu.student.level ASC, edu.student.score DESC ' \
                   'LIMIT 10'
        return cls.Case(query, expected)


class TestSubquery(Parser):
    """SQL parser subquery unit test.
    """
    @classmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = frame.Query(student.select(student.surname, student.score)).select(student.surname)
        expected = 'SELECT edu.student.surname FROM (SELECT edu.student.surname, edu.student.score FROM edu.student)'
        return cls.Case(query, expected)


class TestReference(Parser):
    """SQL parser reference unit test.
    """
    @classmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Parser.Case:
        student = student.reference('foo')
        subquery = student.join(school, student.school == school.sid)\
            .select(student.surname, school.name).reference('bar')
        query = subquery.select(subquery.surname)
        expected = 'SELECT bar.surname FROM (SELECT foo.surname, edu.school.name FROM edu.student AS foo ' \
                   'INNER JOIN edu.school ON foo.school = edu.school.id) AS bar'
        return cls.Case(query, expected)
