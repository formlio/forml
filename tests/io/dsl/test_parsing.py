"""
ETL code statement tests.
"""
# pylint: disable=no-self-use
import types
import typing

import pytest

from forml.io.dsl import parsing, function
from forml.io.dsl import statement as stmtmod
from forml.io.dsl.schema import series as sermod, frame, kind as kindmod


@pytest.fixture(scope='session')
def sources(person: frame.Table, student: frame.Table, school: frame.Table) -> typing.Mapping[frame.Source, tuple]:
    """Sources mapping fixture.
    """
    return types.MappingProxyType({
        stmtmod.Join(student, person, student.surname == person.surname): tuple(['foo']),
        person: tuple([person]),
        student: tuple([student]),
        school: tuple([school])
    })


@pytest.fixture(scope='session')
def columns() -> typing.Mapping[sermod.Column, tuple]:
    """Columns mapping fixture.
    """
    class Columns:
        """Columns mapping.
        """
        def __getitem__(self, column: sermod.Column) -> tuple:
            if isinstance(column, sermod.Field):
                return tuple([column])
            raise KeyError('Unknown column')
    return Columns()


class Series(parsing.Stack, parsing.Series):
    """Dummy statement wrapping all terms into tuples.
    """
    def __init__(self, columns: typing.Mapping[sermod.Column, tuple]):
        parsing.Stack.__init__(self)
        parsing.Series.__init__(self, columns)

    # pylint: disable=missing-function-docstring
    def generate_literal(self, literal: sermod.Literal) -> tuple:
        return tuple([literal])

    def generate_expression(self, expression: typing.Type[sermod.Expression],
                            arguments: typing.Sequence[tuple]) -> tuple:
        return expression, *arguments

    def generate_alias(self, column: tuple, alias: str) -> tuple:
        return column, alias


@pytest.fixture(scope='function')
def series(columns: typing.Mapping[sermod.Column, tuple]) -> parsing.Series:
    """Series fixture.
    """
    return Series(columns)


@pytest.fixture(scope='function')
def statement(columns: typing.Mapping[sermod.Column, tuple],
              sources: typing.Mapping[frame.Source, tuple]) -> parsing.Statement:
    """Statement fixture.
    """
    class Statement(Series, parsing.Statement):  # pylint: disable=abstract-method
        """Dummy statement wrapping all terms into tuples.
        """
        def __init__(self):
            Series.__init__(self, columns)
            parsing.Statement.__init__(self, sources)

        # pylint: disable=missing-function-docstring
        def generate_join(self, left: tuple, right: tuple, condition: tuple, kind: stmtmod.Join.Kind) -> tuple:
            return left, kind, right, condition

        def generate_set(self, left: tuple, right: tuple, kind: stmtmod.Set.Kind) -> tuple:
            return left, kind, right

        def generate_literal(self, literal: sermod.Literal) -> tuple:
            return tuple([literal])

        def generate_expression(self, expression: typing.Type[sermod.Expression],
                                arguments: typing.Sequence[tuple]) -> tuple:
            return expression, *arguments

        def generate_ordering(self, column: tuple, direction: stmtmod.Ordering.Direction) -> tuple:
            return column, direction

        def generate_query(self, source: tuple, columns: typing.Sequence[tuple],
                           where: typing.Optional[tuple],
                           groupby: typing.Sequence[tuple], having: typing.Optional[tuple],
                           orderby: typing.Sequence[tuple], rows: typing.Optional[stmtmod.Rows]) -> tuple:
            return source, tuple(columns), where, tuple(groupby), having, tuple(orderby), rows

    return Statement()


class TestStatement:
    """ tests.
    """
    def test_target(self, person: frame.Table, student: frame.Table, school: frame.Table, statement: parsing.Statement):
        """Target test.
        """
        query = student.join(person, student.surname == person.surname)\
            .join(school, student.school == school.sid)\
            .select(student.surname, school.name, function.Cast(student.score, kindmod.String()))\
            .where(student.score < 2).orderby(student.level, student.score).limit(10)
        query.accept(statement)
        assert statement.result[0][0] == ('foo', )
        assert statement.result[1] == ((student.surname, ), (school.name, ),
                                       (function.Cast, (student.score, ), kindmod.String()))
