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
PEP249 DB API SQL reader.
"""
import abc
import contextlib
import logging
import re
import typing

from forml.io import payload
from forml.io.dsl import parser as parsmod, function, error
from forml.io.dsl.parser import Source, Column
from forml.io.dsl.struct import series, frame, kind as kindmod
from forml.io.feed import extract

LOGGER = logging.getLogger(__name__)


class Parser(parsmod.Visitor[str, str]):  # pylint: disable=unsubscriptable-object
    """Frame DSL parser producing SQL code."""

    class Expression:
        """Expression generator/formatter."""

        ASSOCIATIVE = re.compile(r"\s*(?:(\S*\()?\s*[^-+*/%\s]+\s*(?(1).*\))|TIMESTAMP *'.+'|DATE *'.+')\s*")

        def __init__(self, template: str, mapper: typing.Optional[typing.Callable[..., typing.Sequence]] = None):
            self._template: str = template
            self._mapper: typing.Optional[typing.Callable[..., typing.Sequence]] = mapper

        def __call__(self, *args: typing.Any) -> str:
            """Actual expression generator.

            Args:
                *args: Expression arguments.

            Returns:
                Generated expression value.
            """

            def clean(arg: str) -> str:
                """Add parentheses if necessary.

                Args:
                    arg: Argument to be cleaned.

                Returns:
                    Clean argument.
                """
                if not self.ASSOCIATIVE.fullmatch(arg):
                    arg = f'({arg})'
                return arg

            if self._mapper:
                args = self._mapper(*args)
            args = [clean(a) for a in args]
            return self._template.format(*args)

    KIND: typing.Mapping[kindmod.Any, str] = {
        kindmod.Boolean(): 'BOOLEAN',
        kindmod.Integer(): 'INTEGER',
        kindmod.Float(): 'DOUBLE',
        kindmod.Decimal(): 'DECIMAL',
        kindmod.String(): 'VARCHAR',
        kindmod.Date(): 'DATE',
        kindmod.Timestamp(): 'TIMESTAMP',
    }

    EXPRESSION: typing.Mapping[type[series.Expression], typing.Callable[..., str]] = {
        function.Addition: Expression('{} + {}'),
        function.Subtraction: Expression('{} - {}'),
        function.Multiplication: Expression('{} * {}'),
        function.Division: Expression('{} / {}'),
        function.Modulus: Expression('{} % {}'),
        function.LessThan: Expression('{} < {}'),
        function.LessEqual: Expression('{} <= {}'),
        function.GreaterThan: Expression('{} > {}'),
        function.GreaterEqual: Expression('{} >= {}'),
        function.Equal: Expression('{} = {}'),
        function.NotEqual: Expression('{} != {}'),
        function.IsNull: Expression('{} IS NULL'),
        function.NotNull: Expression('{} IS NOT NULL'),
        function.And: Expression('{} AND {}'),
        function.Or: Expression('{} OR {}'),
        function.Not: Expression('NOT {}'),
        function.Cast: Expression('CAST({} AS {})', lambda _, k: [_, Parser.KIND[k]]),
        function.Avg: Expression('avg({})'),
        function.Count: Expression('count({})', lambda c=None: [c if c is not None else '*']),
        function.Min: Expression('min({})'),
        function.Max: Expression('max({})'),
        function.Sum: Expression('sum({})'),
        function.Year: Expression('year({})'),
        function.Abs: Expression('abs({})'),
        function.Ceil: Expression('ceil({})'),
        function.Floor: Expression('floor({})'),
    }

    DATE = '%Y-%m-%d'
    TIMESTAMP = '%Y-%m-%d %H:%M:%S.%f'

    def resolve_column(self, column: series.Column) -> str:
        """Resolver falling back to a field name in case of no explicit mapping.

        Args:
            column: Column to be resolved.

        Returns:
            Resolved column.
        """
        try:
            return super().resolve_column(column)
        except error.Mapping as err:
            if isinstance(column, series.Element):
                return column.name
            raise err

    def generate_element(self, origin: str, element: str) -> str:  # pylint: disable=no-self-use
        """Generate a field code.

        Args:
            origin: Field source value.
            element: Field symbol.

        Returns:
            Field representation.
        """
        return f'"{origin}"."{element}"'

    def generate_alias(self, column: str, alias: str) -> str:  # pylint: disable=no-self-use
        """Generate column alias code.

        Args:
            column: Column value.
            alias: Alias to be used for given column.

        Returns:
            Aliased column.
        """
        return f'{column} AS "{alias}"'

    def generate_literal(self, value: typing.Any, kind: kindmod.Any) -> str:
        """Generate a literal value.

        Args:
            value: Literal value instance.
            kind: Literal value type.

        Returns:
            Literal.
        """
        if isinstance(kind, kindmod.String):
            return f"'{value}'"
        if isinstance(kind, kindmod.Numeric):
            return f'{value}'
        if isinstance(kind, kindmod.Timestamp):
            return f"TIMESTAMP '{value.strftime(self.TIMESTAMP)}'"
        if isinstance(kind, kindmod.Date):
            return f"DATE '{value.strftime(self.DATE)}'"
        if isinstance(kind, kindmod.Array):
            return f"ARRAY[{', '.join(self.generate_literal(v, kind.element) for v in value)}]"
        raise error.Unsupported(f'Unsupported literal kind: {kind}')

    def generate_expression(self, expression: type[series.Expression], arguments: typing.Sequence[typing.Any]) -> str:
        """Expression of given arguments.

        Args:
            expression: Operator or function implementing the expression.
            arguments: Expression arguments.

        Returns:
            Expression.
        """
        try:
            return self.EXPRESSION[expression](*arguments)
        except KeyError as err:
            raise error.Unsupported(f'Unsupported expression: {expression}') from err

    JOIN: typing.Mapping[frame.Join.Kind, str] = {
        frame.Join.Kind.LEFT: 'LEFT OUTER JOIN',
        frame.Join.Kind.RIGHT: 'RIGHT OUTER JOIN',
        frame.Join.Kind.INNER: 'JOIN',
        frame.Join.Kind.FULL: 'FULL OUTER JOIN',
        frame.Join.Kind.CROSS: 'CROSS JOIN',
    }

    SET: typing.Mapping[frame.Set.Kind, str] = {
        frame.Set.Kind.UNION: 'UNION',
        frame.Set.Kind.INTERSECTION: 'INTERSECT',
        frame.Set.Kind.DIFFERENCE: 'EXCEPT',
    }

    ORDER: typing.Mapping[series.Ordering.Direction, str] = {
        series.Ordering.Direction.ASCENDING: 'ASC',
        series.Ordering.Direction.DESCENDING: 'DESC',
    }

    class Wrap:
        """Helper class for lexical manipulation."""

        WORD = re.compile(r'\s*(\S*\()?\s*[^\s]+\s*(?(1).*\))')
        QUERY = re.compile(r'\s*SELECT')

        @classmethod
        def word(cls, value: str) -> str:
            """Either a single word or any string within parentheses.

            Args:
                value: String to be forced to word.

            Returns:
                Word value of the input string.
            """
            if not cls.WORD.fullmatch(value):
                value = f'({value})'
            return value

        @classmethod
        def subquery(cls, value: str) -> str:
            """If the value is a SELECT statement, it will be wrapped in parentheses.

            Args:
                value: String to be forced to subquery.

            Returns:
                Input value wrapped to parentheses if a SELECT statement.
            """
            if cls.QUERY.match(value):
                value = f'({value})'
            return value

    def generate_join(self, left: str, right: str, condition: typing.Optional[str], kind: frame.Join.Kind) -> str:
        """Generate target code for a join operation using the left/right terms, condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns:
            Join operation.
        """
        join = f'{left} {self.JOIN[kind]} {right}'
        if condition:
            join += f' ON {condition}'
        return join

    def generate_table(
        self, table: Source, columns: typing.Iterable[Column], predicate: typing.Optional[Column]
    ) -> Source:
        return f'"{table}"'

    def generate_set(self, left: str, right: str, kind: frame.Set.Kind) -> str:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns:
            Set operation.
        """
        return f'{left} {self.SET[kind]} {right}'

    def generate_query(
        self,
        source: str,
        columns: typing.Sequence[str],  # pylint: disable=no-self-use
        where: typing.Optional[str],
        groupby: typing.Sequence[str],
        having: typing.Optional[str],
        orderby: typing.Sequence[tuple[str, series.Ordering.Direction]],
        rows: typing.Optional[frame.Rows],
    ) -> str:
        """Generate query statement code.

        Args:
            source: Source.
            columns: Sequence of selected columns.
            where: Where condition.
            groupby: Sequence of grouping specifiers.
            having: Having condition.
            orderby: Ordering specifier.
            rows: Limit spec tuple.

        Returns:
            Query.
        """
        assert columns, 'Expecting columns'
        query = f'SELECT {", ".join(columns)}\nFROM {self.Wrap.subquery(source)}'
        if where:
            query += f'\nWHERE {where}'
        if groupby:
            query += f'\nGROUP BY {", ".join(groupby)}'
        if having:
            query += f'\nHAVING {having}'
        if orderby:
            ordering = ', '.join(f'{c} {self.ORDER[d]}' for c, d in orderby)
            query += f'\nORDER BY {ordering}'
        if rows:
            query += '\nLIMIT'
            if rows.offset:
                query += f' {rows.offset},'
            query += f' {rows.count}'
        return query

    def generate_reference(self, instance: str, name: str) -> tuple[str, str]:  # pylint: disable=no-self-use
        """Generate a source reference (alias) definition.

        Args:
            instance: Source value to be referenced (aliased).
            name: Reference name (alias).

        Returns:
            Tuple of referenced origin and the bare reference handle both in target code.
        """
        return f'{self.Wrap.word(instance)} AS "{name}"', name


class Reader(extract.Reader[str, str, payload.RowMajor], metaclass=abc.ABCMeta):
    """SQL reader base class for PEP249 compliant DB APIs."""

    @classmethod
    @abc.abstractmethod
    def connection(cls, **kwargs: typing.Any):
        """Create a PEP249 compliant connection instance.

        Args:
            **kwargs: Connection specific keyword arguments.

        Returns:
            Connection instance.
        """

    @classmethod
    def parser(cls, sources: typing.Mapping[frame.Source, str], columns: typing.Mapping[series.Column, str]) -> Parser:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            columns: Column mappings to be used by the parser.

        Returns:
            Parser instance.
        """
        return Parser(sources, columns)

    @classmethod
    def format(cls, data: payload.RowMajor) -> payload.ColumnMajor:
        """PEP249 assumes row oriented results, we need columnar so let's transpose here.

        Args:
            data: Row oriented input.

        Returns:
            Columnar output.
        """
        return payload.transpose(data)

    @classmethod
    def read(cls, statement: str, **kwargs) -> payload.RowMajor:
        """Perform the read operation with the given statement.

        Args:
            statement: Query statement in the reader's native syntax.
            kwargs: Optional reader keyword args.

        Returns:
            Row-oriented data provided by the reader.
        """
        LOGGER.debug('Establishing SQL connection')
        with contextlib.closing(cls.connection(**kwargs)) as connection:
            cursor = connection.cursor()
            LOGGER.debug('Executing SQL query')
            cursor.execute(statement)
            return cursor.fetchall()
