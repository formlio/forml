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
SQLAlchemy based ETL feed.
"""
import datetime
import inspect
import logging
import operator
import typing

import pandas
import sqlalchemy
from pyhive import sqlalchemy_trino as trino
from sqlalchemy import sql, func, types as sqltypes
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.engine import interfaces

from forml.io import payload
from forml.io.dsl import parser as parsmod, function, error
from forml.io.dsl.struct import series, frame, kind as kindmod
from forml.io.feed import extract

LOGGER = logging.getLogger(__name__)


Native = typing.TypeVar('Native')


class Type(typing.Generic[Native], sqltypes.TypeDecorator):
    """Base class for custom types with explicit literal processors for specific dialects."""

    cache_ok = True
    PROCESSOR: typing.Mapping[type[interfaces.Dialect], typing.Callable[[Native], str]] = {}

    def _process(self, value: Native, dialect: interfaces.Dialect) -> str:
        """Type processing implementation.

        Args:
            value: Native value to be processed.
            dialect: Target dialect to process for.

        Returns: Dialect encoded value.
        """
        for base in inspect.getmro(type(dialect)):
            if base in self.PROCESSOR:
                processor = self.PROCESSOR[base]
                break
        else:
            processor = self.impl.literal_processor(dialect)
        return processor(value)

    def process_literal_param(self, value: Native, dialect: interfaces.Dialect):
        return self._process(value, dialect)

    def process_bind_param(self, value, dialect: interfaces.Dialect):
        return self._process(value, dialect)

    def process_result_value(self, value, dialect: interfaces.Dialect):
        return self._process(value, dialect)

    def literal_processor(self, dialect: interfaces.Dialect):
        def processor(value: Native):
            return self._process(value, dialect)

        return processor

    @property
    def python_type(self):
        return self.impl.python_type


class Date(Type[datetime.date]):
    """Custom Date type."""

    impl = sqltypes.Date
    ISOFMT = '%Y-%m-%d'
    PROCESSOR = {
        trino.TrinoDialect: lambda v: f"DATE '{v.strftime(Date.ISOFMT)}'",
        sqlite.SQLiteDialect: lambda v: f"'{v.strftime(Date.ISOFMT)}'",
    }


class DateTime(Type[datetime.datetime]):
    """Custom DateTime type."""

    impl = sqltypes.DateTime
    ISOFMT = '%Y-%m-%d %H:%M:%S.%f'
    PROCESSOR = {
        trino.TrinoDialect: lambda v: f"TIMESTAMP '{v.strftime(DateTime.ISOFMT)}'",
        sqlite.SQLiteDialect: lambda v: f"'{v.strftime(DateTime.ISOFMT)}'",
    }


class Parser(parsmod.Visitor[sql.Selectable, sql.ColumnElement]):  # pylint: disable=unsubscriptable-object
    """Frame DSL parser producing SQLAlchemy select expression."""

    KIND: typing.Mapping[kindmod.Any, sql.ColumnElement] = {
        kindmod.Boolean(): sqltypes.Boolean(),
        kindmod.Integer(): sqltypes.Integer(),
        kindmod.Float(): sqltypes.Float(),
        kindmod.Decimal(): sqltypes.DECIMAL(),
        kindmod.String(): sqltypes.Unicode(),
        kindmod.Date(): Date(),
        kindmod.Timestamp(): DateTime(),
    }

    EXPRESSION: typing.Mapping[type[series.Expression], typing.Callable[..., sql.ColumnElement]] = {
        function.Addition: operator.add,
        function.Subtraction: operator.sub,
        function.Multiplication: operator.mul,
        function.Division: operator.truediv,
        function.Modulus: operator.mod,
        function.LessThan: operator.lt,
        function.LessEqual: operator.le,
        function.GreaterThan: operator.gt,
        function.GreaterEqual: operator.ge,
        function.Equal: operator.eq,
        function.NotEqual: operator.ne,
        function.IsNull: lambda c: c.is_(None),
        function.NotNull: lambda c: c.is_not(None),
        function.And: operator.and_,
        function.Or: operator.or_,
        function.Not: operator.not_,
        function.Cast: lambda c, k: sql.cast(c, Parser.KIND[k]),
        function.Avg: func.avg,
        function.Count: func.count,
        function.Min: func.min,
        function.Max: func.max,
        function.Sum: func.sum,
        function.Year: func.year,
        function.Abs: operator.abs,
        function.Ceil: func.ceil,
        function.Floor: func.floor,
    }

    def resolve_column(self, column: series.Column) -> sql.ColumnElement:
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
                return sql.column(column.name)
            raise err

    def generate_element(
        self, origin: sql.Selectable, element: sql.ColumnElement
    ) -> sql.ColumnElement:  # pylint: disable=no-self-use
        """Generate a field code.

        Args:
            origin: Field source value.
            element: Field symbol.

        Returns:
            Field representation.
        """
        return sql.column(element.name, _selectable=origin)

    def generate_alias(self, column: sql.ColumnElement, alias: str) -> sql.ColumnElement:  # pylint: disable=no-self-use
        """Generate column alias code.

        Args:
            column: Column value.
            alias: Alias to be used for given column.

        Returns:
            Aliased column.
        """
        return column.label(alias)

    def generate_literal(self, value: typing.Any, kind: kindmod.Any) -> sql.ColumnElement:
        """Generate a literal value.

        Args:
            value: Literal value instance.
            kind: Literal value type.

        Returns:
            Literal.
        """
        try:
            return sql.bindparam(None, value, self.KIND[kind])
        except KeyError as err:
            raise error.Unsupported(f'Unsupported literal kind: {kind}') from err

    def generate_expression(
        self, expression: type[series.Expression], arguments: typing.Sequence[typing.Any]
    ) -> sql.ColumnElement:
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

    SET: typing.Mapping[frame.Set.Kind, typing.Callable[[sql.Selectable, sql.Selectable], sql.Selectable]] = {
        frame.Set.Kind.UNION: sql.Select.union,
        frame.Set.Kind.INTERSECTION: sql.Select.intersect,
        frame.Set.Kind.DIFFERENCE: sql.Select.except_,
    }
    ORDER: typing.Mapping[series.Ordering.Direction, typing.Callable[[sql.ColumnElement], sql.ColumnElement]] = {
        series.Ordering.Direction.ASCENDING: sql.ColumnElement.asc,
        series.Ordering.Direction.DESCENDING: sql.ColumnElement.desc,
    }

    def generate_join(
        self,
        left: sql.Selectable,
        right: sql.Selectable,
        condition: typing.Optional[sql.ColumnElement],
        kind: frame.Join.Kind,
    ) -> sql.Selectable:
        """Generate target code for a join operation using the left/right terms, condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns:
            Join operation.
        """
        opts = {
            'onclause': condition if condition is not None else sql.literal(True)
        }  # onclause=literal(True) -> CROSS JOIN
        if kind in {frame.Join.Kind.FULL, frame.Join.Kind.CROSS}:
            opts['full'] = True
        elif kind is not frame.Join.Kind.INNER:
            opts['isouter'] = True
            if kind is frame.Join.Kind.RIGHT:
                left, right = right, left
        return left.join(right, **opts)

    def generate_set(self, left: sql.Selectable, right: sql.Selectable, kind: frame.Set.Kind) -> sql.Selectable:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns:
            Set operation.
        """
        return self.SET[kind](left, right)

    def generate_query(
        self,
        source: sql.Selectable,
        columns: typing.Sequence[sql.ColumnElement],  # pylint: disable=no-self-use
        where: typing.Optional[sql.ColumnElement],
        groupby: typing.Sequence[sql.ColumnElement],
        having: typing.Optional[sql.ColumnElement],
        orderby: typing.Sequence[tuple[sql.ColumnElement, series.Ordering.Direction]],
        rows: typing.Optional[frame.Rows],
    ) -> sql.Selectable:
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
        query = sql.select(columns).select_from(source)
        if where is not None:
            query = query.where(where)
        if groupby:
            query = query.group_by(*groupby)
        if having is not None:
            query = query.having(having)
        if orderby:
            query = query.order_by(*(self.ORDER[d](c) for c, d in orderby))
        if rows:
            query = query.limit(rows.count)
            if rows.offset:
                query = query.offser(rows.offset)
        return query

    def generate_reference(
        self, instance: sql.Selectable, name: str
    ) -> tuple[sql.Selectable, sql.Selectable]:  # pylint: disable=no-self-use
        """Generate a source reference (alias) definition.

        Args:
            instance: Source value to be referenced (aliased).
            name: Reference name (alias).

        Returns:
            Tuple of referenced origin and the bare reference handle both in target code.
        """
        ref = instance.alias(name)
        return ref, ref


class Reader(extract.Reader[sql.Selectable, sql.ColumnElement, pandas.DataFrame]):
    """SQLAlchemy based reader."""

    def __init__(
        self,
        sources: typing.Mapping[frame.Source, parsmod.Source],
        columns: typing.Mapping[series.Column, parsmod.Column],
        connection: typing.Union[str, interfaces.Connectable],
        **kwargs,
    ):
        if isinstance(connection, str):
            connection = sqlalchemy.create_engine(connection)
        super().__init__(sources, columns, **{**kwargs, 'con': connection})

    @classmethod
    def parser(
        cls,
        sources: typing.Mapping[frame.Source, sql.Selectable],
        columns: typing.Mapping[series.Column, sql.ColumnElement],
    ) -> Parser:
        """Return the parser instance of this reader.

        Args:
            sources: Source mappings to be used by the parser.
            columns: Column mappings to be used by the parser.

        Returns:
            Parser instance.
        """
        return Parser(sources, columns)

    @classmethod
    def format(cls, data: pandas.DataFrame) -> payload.ColumnMajor:
        """Pandas is already columnar - just return the underlying array.

        Args:
            data: Pandas dataframe.

        Returns:
            Columnar output.
        """
        return data.values.transpose()

    @classmethod
    def read(cls, statement: sql.Selectable, **kwargs) -> pandas.DataFrame:
        """Perform the read operation with the given statement.

        Args:
            statement: SQLAlchemy select statement.
            kwargs: Pandas read_sql parameters.

        Returns:
            Pandas DataFrame of the requested data.
        """
        LOGGER.debug('Submitting SQL query')
        return pandas.read_sql(statement, **kwargs)
