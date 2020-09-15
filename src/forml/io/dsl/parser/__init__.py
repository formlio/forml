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
ETL DSL parser.
"""
import abc
import functools
import logging
import types
import typing

from forml.io.dsl import error
from forml.io.dsl.schema import series as sermod, frame, visit

LOGGER = logging.getLogger(__name__)

Source = typing.TypeVar('Source')
Column = typing.TypeVar('Column')
Symbol = typing.TypeVar('Symbol')


class Stack(typing.Generic[Symbol]):
    """Stack as a base parser structure.

    When used as a context manager the stack structure is exclusive to given context and is checked for total depletion
    on exit.
    """
    def __init__(self):
        self._values: typing.List[Symbol] = list()
        self._context: typing.List[typing.List[Symbol]] = list()

    def __enter__(self) -> 'Stack':
        self._context.append(self._values)
        self._values = list()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self._values) > 0:
            raise RuntimeError('Stack not depleted')
        self._values = self._context.pop()

    def push(self, item: Symbol) -> None:
        """Push new parsed item to the stack.

        Args:
            item: Item to be added.
        """
        self._values.append(item)

    def pop(self) -> Symbol:
        """Remove and return a value from the top of the stack.

        Returns: Item from the stack top.
        """
        return self._values.pop()


def bypass(override: typing.Callable[[Stack, typing.Any], Source]) -> typing.Callable:
    """Bypass the (result of) the particular visit_* implementation if the supplied override resolver provides an
    alternative value.

    Args:
        override: Callable resolver that returns an explicit value for given subject or raises KeyError for unknown
        mapping.

    Returns: Visitor method decorator.
    """
    def decorator(method: typing.Callable[[Stack, typing.Any], None]) -> typing.Callable:
        """Visitor method decorator with added bypassing capability.

        Args:
            method: Visitor method to be decorated.

        Returns: Decorated version of the visit_* method.
        """
        @functools.wraps(method)
        def wrapped(self: Stack, subject: typing.Any) -> None:
            """Decorated version of the visit_* method.

            Args:
                self: Visitor instance.
                subject: Visited subject.
            """
            method(self, subject)
            try:
                new = override(self, subject)
            except error.Mapping:
                pass
            else:
                old = self.pop()
                LOGGER.debug('Overriding result for %s (%s -> %s)', subject, old, new)
                self.push(new)

        return wrapped
    return decorator


class Frame(typing.Generic[Source, Column], Stack[Source], visit.Frame, metaclass=abc.ABCMeta):
    """Frame source parser.
    """
    def __init__(self, sources: typing.Mapping[frame.Source, Source], series: 'Series[Source, Column]'):
        super().__init__()
        self._sources: typing.Mapping[frame.Source, Source] = types.MappingProxyType(sources)
        self._series: Series = series

    @functools.lru_cache()
    def generate_column(self, column: sermod.Column) -> Source:
        """Generate target code for the generic column type.

        Args:
            column: Column instance

        Returns: Column in target code.
        """
        with self._series as visitor:
            column.accept(visitor)
            return visitor.pop()

    def resolve_source(self, source: frame.Source) -> Source:
        """Get a custom target code for a source type.

        Args:
            source: Source instance.

        Returns: Target code for the source instance.
        """
        try:
            return self._sources[source]
        except KeyError as err:
            raise error.Mapping(f'Unknown mapping for source {source}') from err

    @abc.abstractmethod
    def generate_join(self, left: Source, right: Source, condition: typing.Optional[Column],
                      kind: frame.Join.Kind) -> Source:
        """Generate target code for a join operation using the left/right terms, given condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns: Target code for the join operation.
        """

    @abc.abstractmethod
    def generate_set(self, left: Source, right: Source, kind: frame.Set.Kind) -> Source:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns: Target code for the set operation.
        """

    @abc.abstractmethod
    def generate_query(self, source: Source, columns: typing.Sequence[Column],
                       where: typing.Optional[Column], groupby: typing.Sequence[Column],
                       having: typing.Optional[Column], orderby: typing.Sequence[Column],
                       rows: typing.Optional[frame.Rows]) -> Source:
        """Generate query statement code.

        Args:
            source: Source already in target code.
            columns: Sequence of selected columns in target code.
            where: Where condition in target code.
            groupby: Sequence of grouping specifiers in target code.
            having: Having condition in target code.
            orderby: Sequence of ordering specifiers in target code.
            rows: Limit spec tuple.

        Returns: Query in target code.
        """

    @abc.abstractmethod
    def generate_ordering(self, column: Column, direction: sermod.Ordering.Direction) -> Source:
        """Generate column ordering code.

        Args:
            column: Column value already in target code.
            direction: Ordering direction spec.

        Returns: Column ordering in target code.
        """

    @abc.abstractmethod
    def generate_reference(self, instance: Source, name: str) -> Source:
        """Generate reference code.

        Args:
            instance: Instance value already in target code.
            name: Reference name.

        Returns: Instance reference in target code.
        """

    def visit_table(self, source: frame.Table) -> None:
        self.push(self.resolve_source(source))

    @bypass(resolve_source)
    def visit_join(self, source: frame.Join) -> None:
        right = self.pop()
        left = self.pop()
        expression = self.generate_column(source.condition) if source.condition is not None else None
        self.push(self.generate_join(left, right, expression, source.kind))

    @bypass(resolve_source)
    def visit_set(self, source: frame.Set) -> None:
        right = self.pop()
        left = self.pop()
        self.push(self.generate_set(left, right, source.kind))

    @bypass(resolve_source)
    def visit_query(self, source: frame.Query) -> None:
        where = self.generate_column(source.prefilter) if source.prefilter is not None else None
        groupby = [self.generate_column(c) for c in source.grouping]
        having = self.generate_column(source.postfilter) if source.postfilter is not None else None
        orderby = [self.generate_ordering(self.generate_column(c), o) for c, o in source.ordering]
        columns = [self.generate_column(c) for c in source.columns]
        self.push(self.generate_query(self.pop(), columns, where, groupby, having, orderby, source.rows))

    def visit_reference(self, source: frame.Reference) -> None:
        self.push(self.generate_reference(self.pop(), source.name))


class Series(Frame[Source, Column], visit.Series, metaclass=abc.ABCMeta):
    """Series column parser.
    """
    def __init__(self, sources: typing.Mapping[frame.Source, Source], columns: typing.Mapping[sermod.Column, Column]):
        self._columns: typing.Mapping[sermod.Column, Column] = types.MappingProxyType(columns)
        super().__init__(sources, self)

    def resolve_column(self, column: sermod.Field) -> Column:
        """Get a custom target code for a column value.

        Args:
            column: Column instance.

        Returns: Column in target code representation.
        """
        try:
            return self._columns[column]
        except KeyError as err:
            raise error.Mapping(f'Unknown mapping for column {column}') from err

    @abc.abstractmethod
    def generate_field(self, source: Source, field: Column) -> Column:
        """Generate a field code.

        Args:
            source: Column value already in target code.
            field: Field symbol to be used for given column.

        Returns: Field in target code.
        """

    @abc.abstractmethod
    def generate_alias(self, column: Column, alias: str) -> Column:
        """Generate column alias code.

        Args:
            column: Column value already in target code.
            alias: Alias to be used for given column.

        Returns: Aliased column in target code.
        """

    @abc.abstractmethod
    def generate_literal(self, literal: sermod.Literal) -> Column:
        """Generate target code for a literal value.

        Args:
            literal: Literal value instance.

        Returns: Literal in target code representation.
        """

    @abc.abstractmethod
    def generate_expression(self, expression: typing.Type[sermod.Expression],
                            arguments: typing.Sequence[Column]) -> Column:
        """Generate target code for an expression of given arguments.

        Args:
            expression: Operator or function implementing the expression.
            arguments: Expression arguments.

        Returns: Expression in target code representation.
        """

    def visit_field(self, column: sermod.Field) -> None:
        self.push(self.generate_field(self.pop(), self.resolve_column(column)))

    @bypass(resolve_column)
    def visit_aliased(self, column: sermod.Aliased) -> None:
        self.push(self.generate_alias(self.pop(), column.name))

    @bypass(resolve_column)
    def visit_literal(self, column: sermod.Literal) -> None:
        self.push(self.generate_literal(column))

    @bypass(resolve_column)
    def visit_expression(self, column: sermod.Expression) -> None:
        arguments = tuple(reversed([self.pop() if isinstance(c, sermod.Column) else c for c in reversed(column)]))
        self.push(self.generate_expression(column.__class__, arguments))