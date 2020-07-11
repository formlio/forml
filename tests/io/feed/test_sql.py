"""
SQL feed tests.
"""
# pylint: disable=no-self-use
import abc
import types
import typing

import pytest

from forml.io.dsl import statement, function
from forml.io.dsl.schema import series as sermod, frame
from forml.io.feed import sql


class Parser(metaclass=abc.ABCMeta):
    """SQL parser unit tests base class.
    """
    class Case(typing.NamedTuple):
        """Test case input/output.
        """
        query: statement.Query
        expected: str

        def __call__(self, parser: sql.Feed.Reader.Parser):
            def strip(value: str) -> str:
                """Replace all whitespace with single space.
                """
                return ' '.join(value.strip().split())

            self.query.accept(parser)
            assert strip(parser.result) == strip(self.expected)

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
    @pytest.fixture(scope='session')
    def columns(sources: typing.Mapping[frame.Source, str]) -> typing.Mapping[sermod.Column, str]:
        """Columns mapping fixture.
        """

        class Columns:
            """Columns mapping.
            """

            def __getitem__(self, column: sermod.Column) -> tuple:
                if isinstance(column, sermod.Field):
                    return f'{sources[column.table]}.{column.name}'
                raise KeyError('Unknown column')

        return Columns()

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(columns: typing.Mapping[sermod.Column, str],
               sources: typing.Mapping[frame.Source, str]) -> sql.Feed.Reader.Parser:
        """Parser fixture.
        """
        return sql.Feed.Reader.Parser(columns, sources)

    @classmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def case(cls, student: frame.Table, school: frame.Table) -> Case:
        """Case fixture.
        """

    def test_parse(self, parser: sql.Feed.Reader.Parser, case: Case):
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


class TestJoin(Parser):
    """SQL parser join unit test.
    """
    @classmethod
    @pytest.fixture(scope='session', params=(Parser.Case(None, 'LEFT'),
                                             Parser.Case(statement.Join.Kind.LEFT, 'LEFT'),
                                             Parser.Case(statement.Join.Kind.RIGHT, 'RIGHT'),
                                             Parser.Case(statement.Join.Kind.FULL, 'FULL'),
                                             Parser.Case(statement.Join.Kind.INNER, 'INNER'),
                                             Parser.Case(statement.Join.Kind.CROSS, 'CROSS')))
    def case(cls, request, student: frame.Table, school: frame.Table) -> Parser.Case:
        query = student.join(school, student.school == school.sid, kind=request.param.query)\
            .select(student.surname, school.name)
        expected = 'SELECT edu.student.surname, edu.school.name ' \
                   f'FROM edu.student {request.param.expected} JOIN edu.school ON edu.student.school = edu.school.id'
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
            .where(student.score < 2).orderby(student.level, student.score).limit(10)
        expected = 'SELECT edu.student.surname AS student, count(edu.school.name) ' \
                   'FROM edu.student LEFT JOIN edu.school ON edu.student.school = edu.school.id ' \
                   'WHERE edu.student.score < 2 GROUP BY edu.student.surname HAVING count(edu.school.name) > 1 ' \
                   'ORDER BY edu.student.level ASC, edu.student.score ASC ' \
                   'LIMIT 10'
        return cls.Case(query, expected)
