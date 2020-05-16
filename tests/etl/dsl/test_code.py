"""
ETL code transpiler tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.etl.dsl import code, statement
from forml.etl.dsl.schema import series, frame


@pytest.fixture(scope='function')
def transpiler() -> code.Transpiler:
    """Transpiler fixture.
    """
    class Transpiler(code.Transpiler[tuple]):  # pylint: disable=unsubscriptable-object
        """Dummy transpiler wrapping all terms into tuples.
        """
        class Tables:
            """Identity tables mapper.
            """
            def __getitem__(self, item):
                return item

        def __init__(self):
            super().__init__(self.Tables())

        # pylint: disable=missing-function-docstring
        def generate_join(self, left: tuple, right: tuple, condition: tuple, kind: statement.Join.Kind) -> tuple:
            return left, kind, right, condition

        def generate_set(self, left: tuple, right: tuple, kind: statement.Set.Kind) -> tuple:
            return left, kind, right

        def generate_field(self, field: series.Field) -> tuple:
            return tuple([field])

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
            return source, columns, where, groupby, having, orderby, rows

    return Transpiler()


class TestTranspiler:
    """Transpiler tests.
    """
    def test_target(self, student: frame.Table, school: frame.Table, transpiler: code.Transpiler):
        """Target test.
        """
        query = student.join(school, student.school == school.sid).select(student.surname, school.name)\
            .where(student.score < 2).orderby(student.level, student.score).limit(10)
        query.accept(transpiler)
        print(transpiler.target)
