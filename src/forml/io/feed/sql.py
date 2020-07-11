"""
ANSI SQL ETL feed.
"""
import abc
import contextlib
import typing

from forml import io
from forml.io.dsl import parsing, statement as stmtmod
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
            JOIN = {stmtmod.Join.Kind.LEFT: 'LEFT',
                    stmtmod.Join.Kind.RIGHT: 'RIGHT',
                    stmtmod.Join.Kind.INNER: 'INNER',
                    stmtmod.Join.Kind.FULL: 'FULL',
                    stmtmod.Join.Kind.CROSS: 'CROSS'}

            SET = {stmtmod.Set.Kind.UNION: 'UNION',
                   stmtmod.Set.Kind.INTERSECTION: 'INTERSECT',
                   stmtmod.Set.Kind.DIFFERENCE: 'EXCEPT'}

            ORDER = {stmtmod.Ordering.Direction.ASCENDING: 'ASC',
                     stmtmod.Ordering.Direction.DESCENDING: 'DESC'}

            DATE = '%Y-%m-%d'
            TIMESTAMP = '%Y-%m-%d %H:%M:%S.%f'

            def __init__(self, columns: typing.Mapping[series.Column, str],
                         sources: typing.Mapping[frame.Source, str]):
                parsing.Stack.__init__(self)
                parsing.Series[str].__init__(self, columns)
                parsing.Statement[str].__init__(self, sources)

            def generate_alias(self, column: str, alias: str) -> str:  # pylint: disable=no-self-use
                return f'{column} AS {alias}'

            def generate_literal(self, literal: series.Literal) -> str:
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
                raise RuntimeError(f'Unsupported literal kind {literal.kind}')

            def generate_expression(self, expression: typing.Type[series.Expression],
                                    arguments: typing.Sequence[str]) -> str:
                pass

            def generate_join(self, left: str, right: str, condition: str, kind: stmtmod.Join.Kind) -> str:
                return f'{left} {self.JOIN[kind]} {right} ON {condition}'

            def generate_set(self, left: str, right: str, kind: stmtmod.Set.Kind) -> str:
                return f'{left} {self.SET[kind]} {right}'

            def generate_query(self, source: str, columns: typing.Sequence[str],  # pylint: disable=no-self-use
                               where: typing.Optional[str],
                               groupby: typing.Sequence[str], having: typing.Optional[str],
                               orderby: typing.Sequence[str], rows: typing.Optional[stmtmod.Rows]) -> str:
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
                return f'{column} {self.ORDER[direction]}'

        @classmethod
        @abc.abstractmethod
        def connection(cls, **kwargs: typing.Any) -> typing.Any:
            """Create a PEP249 compliant connection instance.

            Args:
                **kwargs:

            Returns: Connection instance.
            """

        @classmethod
        def read(cls, statement: parsing.ResultT, **kwargs) -> typing.Sequence[typing.Sequence[typing.Any]]:
            with contextlib.closing(cls.connection(**kwargs)) as connection:
                cursor = connection.cursor()
                cursor.execute(statement)
                return cursor.fetchall()

        @classmethod
        def parser(cls, sources: typing.Mapping[frame.Source, str],
                   columns: typing.Mapping[series.Column, str]) -> parsing.Statement:
            return cls.Parser(columns, sources)

    @classmethod
    def reader(cls, sources: typing.Mapping['frame.Source', str],
               columns: typing.Mapping['series.Column', str],
               **kwargs) -> typing.Callable[['stmtmod.Query'], typing.Sequence[typing.Sequence[typing.Any]]]:
        return cls.Reader(sources, columns, **kwargs)  # pylint: disable=abstract-class-instantiated
