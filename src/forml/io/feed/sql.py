"""
ANSI SQL ETL feed.
"""
import abc
import contextlib
import typing

from forml import io
from forml.io.dsl import parsing, statement as stmtmod, function, error
from forml.io.dsl.schema import series, frame, kind as kindmod
from forml.io.etl import extract


class Feed(io.Feed):
    """SQL feed.
    """
    class Reader(extract.Reader, metaclass=abc.ABCMeta):
        """SQL reader base class for PEP249 compliant DB APIs.
        """
        class Parser(parsing.Stack, parsing.Series[str], parsing.Statement[str]):
            """DSL parser producing an ANSI SQL select statements.
            """
            EXPRESSION: typing.Mapping[typing.Type[series.Expression], typing.Callable[..., str]] = {
                function.LessThan: lambda a, b: f'{a} < {b}',
                function.LessEqual: lambda a, b: f'{a} <= {b}',
                function.GreaterThan: lambda a, b: f'{a} > {b}',
                function.GreaterEqual: lambda a, b: f'{a} >= {b}',
                function.Equal: lambda a, b: f'{a} = {b}',
                function.NotEqual: lambda a, b: f'{a} != {b}',
                function.IsNull: lambda a: f'{a} IS NULL',
                function.NotNull: lambda a: f'{a} IS NOT NULL',
                function.And: lambda a, b: f'{a} AND {b}',
                function.Or: lambda a, b: f'{a} OR {b}',
                function.Not: lambda a: f'NOT {a}',
                function.Cast: lambda a, b: f'cast({a} AS {b})',
                function.Count: lambda a=None: f'count({a if a is not None else "*"})'
            }

            JOIN: typing.Mapping[stmtmod.Join.Kind, str] = {
                stmtmod.Join.Kind.LEFT: 'LEFT',
                stmtmod.Join.Kind.RIGHT: 'RIGHT',
                stmtmod.Join.Kind.INNER: 'INNER',
                stmtmod.Join.Kind.FULL: 'FULL',
                stmtmod.Join.Kind.CROSS: 'CROSS'
            }

            SET: typing.Mapping[stmtmod.Set.Kind, str] = {
                stmtmod.Set.Kind.UNION: 'UNION',
                stmtmod.Set.Kind.INTERSECTION: 'INTERSECT',
                stmtmod.Set.Kind.DIFFERENCE: 'EXCEPT'
            }

            ORDER: typing.Mapping[stmtmod.Ordering.Direction, str] = {
                stmtmod.Ordering.Direction.ASCENDING: 'ASC',
                stmtmod.Ordering.Direction.DESCENDING: 'DESC'
            }

            DATE = '%Y-%m-%d'
            TIMESTAMP = '%Y-%m-%d %H:%M:%S.%f'

            def __init__(self, columns: typing.Mapping[series.Column, str], sources: typing.Mapping[frame.Source, str]):
                parsing.Stack.__init__(self)
                # pylint: disable=non-parent-init-called (#3505)
                parsing.Series.__init__(self, columns)
                parsing.Statement.__init__(self, sources)

            def generate_alias(self, column: str, alias: str) -> str:  # pylint: disable=no-self-use
                """Generate column alias code.

                Args:
                    column: Column value.
                    alias: Alias to be used for given column.

                Returns: Aliased column.
                """
                return f'{column} AS {alias}'

            def generate_literal(self, literal: series.Literal) -> str:
                """Generate a literal value.

                Args:
                    literal: Literal value instance.

                Returns: Literal.
                """
                if isinstance(literal.kind, kindmod.String):
                    return f"'{literal.value}'"
                if isinstance(literal.kind, kindmod.Numeric):
                    return f'{literal.value}'
                if isinstance(literal.kind, kindmod.Date):
                    return f"DATE '{literal.value.strptime(self.DATE)}'"
                if isinstance(literal.kind, kindmod.Timestamp):
                    return f"TIMESTAMP '{literal.value.strptime(self.TIMESTAMP)}'"
                if isinstance(literal.kind, kindmod.Array):
                    return f"ARRAY[{', '.join(self.generate_literal(v) for v in literal.value)}]"
                raise error.Unsupported(f'Unsupported literal kind: {literal.kind}')

            def generate_expression(self, expression: typing.Type[series.Expression],
                                    arguments: typing.Sequence[str]) -> str:
                """Expression of given arguments.

                Args:
                    expression: Operator or function implementing the expression.
                    arguments: Expression arguments.

                Returns: Expression.
                """
                try:
                    return self.EXPRESSION[expression](*arguments)
                except KeyError:
                    raise error.Unsupported(f'Unsupported expression: {expression}')

            def generate_join(self, left: str, right: str, condition: str, kind: stmtmod.Join.Kind) -> str:
                """Generate target code for a join operation using the left/right terms, condition and a join type.

                Args:
                    left: Left side of the join pair.
                    right: Right side of the join pair.
                    condition: Join condition.
                    kind: Join type.

                Returns: Join operation.
                """
                return f'{left} {self.JOIN[kind]} JOIN {right} ON {condition}'

            def generate_set(self, left: str, right: str, kind: stmtmod.Set.Kind) -> str:
                """Generate target code for a set operation using the left/right terms, given a set type.

                Args:
                    left: Left side of the set pair.
                    right: Right side of the set pair.
                    kind: Set type.

                Returns: Set operation.
                """
                return f'{left} {self.SET[kind]} {right}'

            def generate_query(self, source: str, columns: typing.Sequence[str],  # pylint: disable=no-self-use
                               where: typing.Optional[str],
                               groupby: typing.Sequence[str], having: typing.Optional[str],
                               orderby: typing.Sequence[str], rows: typing.Optional[stmtmod.Rows]) -> str:
                """Generate query statement code.

                Args:
                    source: Source.
                    columns: Sequence of selected columns.
                    where: Where condition.
                    groupby: Sequence of grouping specifiers.
                    having: Having condition.
                    orderby: Sequence of ordering specifiers.
                    rows: Limit spec tuple.

                Returns: Query.
                """
                assert columns, 'Expecting columns'
                query = f'SELECT {", ".join(columns)}\nFROM {source}'
                if where:
                    query += f'\nWHERE {where}'
                if groupby:
                    query += f'\nGROUP BY {", ".join(groupby)}'
                if having:
                    query += f'\nHAVING {having}'
                if orderby:
                    query += f'\nORDER BY {", ".join(orderby)}'
                if rows:
                    query += '\nLIMIT'
                    if rows.offset:
                        query += f' {rows.offset},'
                    query += f' {rows.count}'
                return query

            def generate_ordering(self, column: str, direction: stmtmod.Ordering.Direction) -> str:
                """Generate column ordering.

                Args:
                    column: Column value.
                    direction: Ordering direction spec.

                Returns: Column ordering.
                """
                return f'{column} {self.ORDER[direction]}'

        @classmethod
        @abc.abstractmethod
        def connection(cls, **kwargs: typing.Any):
            """Create a PEP249 compliant connection instance.

            Args:
                **kwargs: Connection specific keyword arguments.

            Returns: Connection instance.
            """

        @classmethod
        def read(cls, statement: parsing.ResultT, **kwargs) -> extract.Columnar:
            with contextlib.closing(cls.connection(**kwargs)) as connection:
                cursor = connection.cursor()
                cursor.execute(statement)
                return cursor.fetchall()

        @classmethod
        def parser(cls, sources: typing.Mapping[frame.Source, str],
                   columns: typing.Mapping[series.Column, str]) -> parsing.Statement:
            return cls.Parser(columns, sources)

    @classmethod
    def reader(cls, sources: typing.Mapping['frame.Source', 'parsing.ResultT'],
               columns: typing.Mapping['series.Column', 'parsing.ResultT'],
               **kwargs: typing.Any) -> typing.Callable[[stmtmod.Query], extract.Columnar]:
        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated