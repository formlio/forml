"""
ETL code visitor tests.
"""
# pylint: disable=no-self-use
import types
import typing

import pytest

from forml.etl.dsl import parsing, statement
from forml.etl.dsl.schema import series, frame


@pytest.fixture(scope='session')
def sources(person: frame.Table, student: frame.Table, school: frame.Table) -> typing.Mapping[frame.Source, tuple]:
    """Sources mapping fixture.
    """
    return types.MappingProxyType({
        statement.Join(student, person, student.surname == person.surname): tuple(['foo']),
        person: tuple([person]),
        student: tuple([student]),
        school: tuple([school])
    })


@pytest.fixture(scope='session')
def columns() -> typing.Mapping[series.Column, tuple]:
    """Columns mapping fixture.
    """
    class Columns:
        """Columns mapping.
        """
        def __getitem__(self, column: series.Column) -> tuple:
            if isinstance(column, series.Field):
                return tuple([column])
            raise KeyError('Unknown column')
    return Columns()


@pytest.fixture(scope='function')
def visitor(sources: typing.Mapping[frame.Source, tuple],
            columns: typing.Mapping[series.Column, tuple]) -> parsing.Visitor:
    """Visitor fixture.
    """
    class Visitor(parsing.Visitor[tuple]):  # pylint: disable=unsubscriptable-object
        """Dummy visitor wrapping all terms into tuples.
        """
        def __init__(self):
            super().__init__(sources, columns)

        # pylint: disable=missing-function-docstring
        def generate_join(self, left: tuple, right: tuple, condition: tuple, kind: statement.Join.Kind) -> tuple:
            return left, kind, right, condition

        def generate_set(self, left: tuple, right: tuple, kind: statement.Set.Kind) -> tuple:
            return left, kind, right

        def generate_literal(self, literal: series.Literal) -> tuple:
            return tuple([literal])

        def generate_expression(self, expression: typing.Type[series.Expression],
                                arguments: typing.Sequence[tuple]) -> tuple:
            return expression, *arguments

        def generate_alias(self, column: tuple, alias: str) -> tuple:
            return column, alias

        def generate_ordering(self, column: tuple, direction: statement.Ordering.Direction) -> tuple:
            return column, direction

        def generate_query(self, source: tuple, columns: typing.Sequence[tuple],
                           where: typing.Optional[tuple],
                           groupby: typing.Sequence[tuple], having: typing.Optional[tuple],
                           orderby: typing.Sequence[tuple], rows: typing.Optional[statement.Rows]) -> tuple:
            return source, tuple(columns), where, tuple(groupby), having, tuple(orderby), rows

    return Visitor()


class TestVisitor:
    """ tests.
    """
    def test_target(self, person: frame.Table, student: frame.Table, school: frame.Table, visitor: parsing.Visitor):
        """Target test.
        """
        query = student.join(person, student.surname == person.surname)\
            .join(school, student.school == school.sid).select(student.surname, school.name)\
            .where(student.score < 2).orderby(student.level, student.score).limit(10)
        query.accept(visitor)
        assert visitor.result[0][0] == ('foo', )
        assert visitor.result[1] == ((student.surname, ), (school.name, ))
