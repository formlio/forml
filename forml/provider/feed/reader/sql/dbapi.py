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

from forml import io
from forml.io import dsl, layout
from forml.io.dsl import function
from forml.io.dsl import parser as parsmod

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

    KIND: typing.Mapping[dsl.Any, str] = {
        dsl.Boolean(): 'BOOLEAN',
        dsl.Integer(): 'INTEGER',
        dsl.Float(): 'DOUBLE',
        dsl.Decimal(): 'DECIMAL',
        dsl.String(): 'VARCHAR',
        dsl.Date(): 'DATE',
        dsl.Timestamp(): 'TIMESTAMP',
    }

    EXPRESSION: typing.Mapping[type[dsl.Expression], typing.Callable[..., str]] = {
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

    def resolve_feature(self, feature: dsl.Feature) -> str:
        """Resolver falling back to a field name in case of no explicit mapping.

        Args:
            feature: Column to be resolved.

        Returns:
            Resolved feature.
        """
        try:
            return super().resolve_feature(feature)
        except dsl.UnprovisionedError as err:
            if isinstance(feature, dsl.Element):
                return feature.name
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

    def generate_alias(self, feature: str, alias: str) -> str:  # pylint: disable=no-self-use
        """Generate feature alias code.

        Args:
            feature: Column value.
            alias: Alias to be used for given feature.

        Returns:
            Aliased feature.
        """
        return f'{feature} AS "{alias}"'

    def generate_literal(self, value: typing.Any, kind: dsl.Any) -> str:
        """Generate a literal value.

        Args:
            value: Literal value instance.
            kind: Literal value type.

        Returns:
            Literal.
        """
        if isinstance(kind, dsl.String):
            return f"'{value}'"
        if isinstance(kind, dsl.Numeric):
            return f'{value}'
        if isinstance(kind, dsl.Timestamp):
            return f"TIMESTAMP '{value.strftime(self.TIMESTAMP)}'"
        if isinstance(kind, dsl.Date):
            return f"DATE '{value.strftime(self.DATE)}'"
        if isinstance(kind, dsl.Array):
            return f"ARRAY[{', '.join(self.generate_literal(v, kind.element) for v in value)}]"
        raise dsl.UnsupportedError(f'Unsupported literal kind: {kind}')

    def generate_expression(self, expression: type[dsl.Expression], arguments: typing.Sequence[typing.Any]) -> str:
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
            raise dsl.UnsupportedError(f'Unsupported expression: {expression}') from err

    JOIN: typing.Mapping[dsl.Join.Kind, str] = {
        dsl.Join.Kind.LEFT: 'LEFT OUTER JOIN',
        dsl.Join.Kind.RIGHT: 'RIGHT OUTER JOIN',
        dsl.Join.Kind.INNER: 'JOIN',
        dsl.Join.Kind.FULL: 'FULL OUTER JOIN',
        dsl.Join.Kind.CROSS: 'CROSS JOIN',
    }

    SET: typing.Mapping[dsl.Set.Kind, str] = {
        dsl.Set.Kind.UNION: 'UNION',
        dsl.Set.Kind.INTERSECTION: 'INTERSECT',
        dsl.Set.Kind.DIFFERENCE: 'EXCEPT',
    }

    ORDER: typing.Mapping[dsl.Ordering.Direction, str] = {
        dsl.Ordering.Direction.ASCENDING: 'ASC',
        dsl.Ordering.Direction.DESCENDING: 'DESC',
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

    def generate_join(self, left: str, right: str, condition: typing.Optional[str], kind: dsl.Join.Kind) -> str:
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

    def generate_table(self, table: str, features: typing.Iterable[str], predicate: typing.Optional[str]) -> str:
        return f'"{table}"'

    def generate_set(self, left: str, right: str, kind: dsl.Set.Kind) -> str:
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
        features: typing.Sequence[str],  # pylint: disable=no-self-use
        where: typing.Optional[str],
        groupby: typing.Sequence[str],
        having: typing.Optional[str],
        orderby: typing.Sequence[tuple[str, dsl.Ordering.Direction]],
        rows: typing.Optional[dsl.Rows],
    ) -> str:
        """Generate query statement code.

        Args:
            source: Source.
            features: Sequence of selected features.
            where: Where condition.
            groupby: Sequence of grouping specifiers.
            having: Having condition.
            orderby: Ordering specifier.
            rows: Limit spec tuple.

        Returns:
            Query.
        """
        assert features, 'Expecting features'
        query = f'SELECT {", ".join(features)}\nFROM {self.Wrap.subquery(source)}'
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


class Reader(io.Feed.Reader[str, str, layout.RowMajor], metaclass=abc.ABCMeta):
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
    def parser(cls, sources: typing.Mapping[dsl.Source, str], features: typing.Mapping[dsl.Feature, str]) -> Parser:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            features: Column mappings to be used by the parser.

        Returns:
            Parser instance.
        """
        return Parser(sources, features)

    @classmethod
    def read(cls, statement: str, **kwargs) -> layout.RowMajor:
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
