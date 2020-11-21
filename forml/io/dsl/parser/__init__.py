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
import collections
import functools
import logging
import types
import typing

from forml.io.dsl import error
from forml.io.dsl.struct import series as sermod, frame, visit, kind as kindmod

LOGGER = logging.getLogger(__name__)

Source = typing.TypeVar('Source')
Column = typing.TypeVar('Column')
Symbol = typing.TypeVar('Symbol')


class Container(typing.Generic[Symbol]):
    """Base parser structure.

    When used as a context manager the internal structure is exclusive to given context and is checked for total
    depletion on exit.
    """

    class Context:
        """Storage context."""

        class Symbols:
            """Stack for parsed symbols."""

            def __init__(self):
                self._stack: typing.List[Symbol] = list()

            def __bool__(self):
                return bool(self._stack)

            def push(self, item: Symbol) -> None:
                """Push new parsed item to the stack.

                Args:
                    item: Item to be added.
                """
                self._stack.append(item)

            def pop(self) -> Symbol:
                """Remove and return a value from the top of the stack.

                Returns:
                    Item from the stack top.
                """
                if not self._stack:
                    raise RuntimeError('Empty context')
                return self._stack.pop()

        def __init__(self):
            self.symbols: Container.Context.Symbols = self.Symbols()

        @property
        def dirty(self) -> bool:
            """Check the context is safe to be closed.

            Returns:
                True if not safe for closing.
            """
            return bool(self.symbols)

    def __init__(self):
        self._context: typing.Optional[Container.Context] = None
        self._stack: typing.List[Container.Context] = list()

    @property
    def context(self) -> 'Container.Context':
        """Context accessor."""
        if not self._context:
            raise RuntimeError('Invalid context')
        return self._context

    def __enter__(self) -> 'Container':
        self._stack.append(self._context)
        self._context = self.Context()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            return
        if self._context and self._context.dirty:
            raise RuntimeError('Context not fetched')
        self._context = self._stack.pop()

    def fetch(self) -> Symbol:
        """Storage retrieval. Must be called exactly once and at the point where there is exactly one symbol pending
        in the context. Successful fetch will kill the context.

        Returns:
            Last symbol from the context.
        """
        symbol = self.context.symbols.pop()
        if self.context.dirty:
            raise RuntimeError('Premature fetch')
        self._context = None
        return symbol


def bypass(override: typing.Callable[[Container, typing.Any], Source]) -> typing.Callable:
    """Bypass the (result of) the particular visit_* implementation if the supplied override resolver provides an
    alternative value.

    Args:
        override: Callable resolver that returns an explicit value for given subject or raises KeyError for unknown
        mapping.

    Returns:
        Visitor method decorator.
    """

    def decorator(method: typing.Callable[[Container, typing.Any], typing.ContextManager[None]]) -> typing.Callable:
        """Visitor method decorator with added bypassing capability.

        Args:
            method: Visitor method to be decorated.

        Returns:
            Decorated version of the visit_* method.
        """

        @functools.wraps(method)
        def wrapped(self: Container, subject: typing.Any) -> None:
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
                old = self.context.symbols.pop()
                LOGGER.debug('Overriding result for %s (%s -> %s)', subject, old, new)
                self.context.symbols.push(new)

        return wrapped

    return decorator


class Columnar(
    typing.Generic[Source, Column], Container[typing.Union[Source, Column]], visit.Columnar, metaclass=abc.ABCMeta
):
    """Base parser class for both Frame and Series visitors."""

    def __init__(self, sources: typing.Mapping[frame.Source, Source]):
        super().__init__()
        self._sources: typing.Mapping[frame.Source, Source] = types.MappingProxyType(sources)

    @abc.abstractmethod
    def generate_column(self, column: sermod.Column) -> Column:
        """Generate target code for the generic column type.

        Args:
            column: Column instance

        Returns:
            Column in target code.
        """

    def resolve_source(self, source: frame.Source) -> Source:
        """Get a custom target code for a source type.

        Args:
            source: Source instance.

        Returns:
            Target code for the source instance.
        """
        try:
            return self._sources[source]
        except KeyError as err:
            raise error.Mapping(f'Unknown mapping for source {source}') from err


class Frame(typing.Generic[Source, Column], Columnar[Source, Column], visit.Frame, metaclass=abc.ABCMeta):
    """Frame source parser."""

    class Series(typing.Generic[Source, Column], Columnar[Source, Column], visit.Series, metaclass=abc.ABCMeta):
        """Series column parser."""

        def __init__(
            self, sources: typing.Mapping[frame.Source, Source], columns: typing.Mapping[sermod.Column, Column]
        ):
            super().__init__(sources)
            self._columns: typing.Mapping[sermod.Column, Column] = types.MappingProxyType(columns)

        def resolve_column(self, column: sermod.Column) -> Column:
            """Get a custom target code for a column value.

            Args:
                column: Column instance.

            Returns:
                Column in target code representation.
            """
            try:
                return self._columns[column]
            except KeyError as err:
                raise error.Mapping(f'Unknown mapping for column {column}') from err

        @functools.lru_cache()
        def generate_column(self, column: sermod.Column) -> Column:
            """Generate target code for the generic column type.

            Args:
                column: Column instance

            Returns:
                Column in target code.
            """
            with self as visitor:
                column.accept(visitor)
                return visitor.fetch()

        @abc.abstractmethod
        def generate_reference(self, name: str) -> Column:
            """Generate reference code.

            Args:
                name: Reference name.

            Returns:
                Instance reference in target code.
            """

        @abc.abstractmethod
        def generate_element(self, origin: Source, element: Column) -> Column:
            """Generate a field code.

            Args:
                origin: Column value already in target code.
                element: Field symbol to be used for given column.

            Returns:
                Field in target code.
            """

        @abc.abstractmethod
        def generate_alias(self, column: Column, alias: str) -> Column:
            """Generate column alias code.

            Args:
                column: Column value already in target code.
                alias: Alias to be used for given column.

            Returns:
                Aliased column in target code.
            """

        @abc.abstractmethod
        def generate_literal(self, value: typing.Any, kind: kindmod.Any) -> Column:
            """Generate target code for a literal value.

            Args:
                value: Literal value instance.
                kind: Literal value type.

            Returns:
                Literal in target code representation.
            """

        @abc.abstractmethod
        def generate_expression(
            self, expression: typing.Type[sermod.Expression], arguments: typing.Sequence[typing.Any]
        ) -> Column:
            """Generate target code for an expression of given arguments.

            Args:
                expression: Operator or function implementing the expression.
                arguments: Expression arguments.

            Returns:
                Expression in target code representation.
            """

        def generate_table(self, table: Source) -> Source:  # pylint: disable=no-self-use
            """Generate a target code for a table instance given its actual field requirements.

            Args:
                table: Table (already in target code based on the provided mapping) to be generated.

            Returns:
                Table target code potentially optimized based on field requirements.
            """
            return table

        def visit_table(self, origin: frame.Table) -> None:
            super().visit_table(origin)
            self.context.symbols.push(self.generate_table(self.resolve_source(origin)))

        def visit_reference(self, origin: frame.Reference) -> None:
            super().visit_reference(origin)
            self.context.symbols.push(self.generate_reference(origin.name))

        def visit_aliased(self, column: sermod.Aliased) -> None:
            super().visit_aliased(column)
            self.context.symbols.push(self.generate_alias(self.context.symbols.pop(), column.name))

        def visit_literal(self, column: sermod.Literal) -> None:
            super().visit_literal(column)
            self.context.symbols.push(self.generate_literal(column.value, column.kind))

        def visit_element(self, column: sermod.Element) -> None:
            super().visit_element(column)
            self.context.symbols.push(self.generate_element(self.context.symbols.pop(), self.resolve_column(column)))

        @bypass(resolve_column)
        def visit_expression(self, column: sermod.Expression) -> None:
            super().visit_expression(column)
            arguments = tuple(
                reversed([self.context.symbols.pop() if isinstance(c, sermod.Column) else c for c in reversed(column)])
            )
            self.context.symbols.push(self.generate_expression(column.__class__, arguments))

        @bypass(resolve_column)
        def visit_window(self, column: sermod.Window) -> typing.ContextManager[None]:
            raise NotImplementedError('Window functions not yet supported')

    class Context(Container.Context):
        """Extended container context for holding the segments."""

        class Tables:
            """Container for segments of all tables."""

            class Segment(collections.namedtuple('Segment', 'fields, factors')):
                """Frame segment specification as a list of columns (vertical) and row predicates (horizontal)."""

                def __new__(cls):
                    return super().__new__(cls, set(), set())

                @property
                def predicate(self) -> typing.Optional[sermod.Predicate]:
                    """Combine the factors into single predicate.

                    Returns:
                        Predicate expression.
                    """
                    return functools.reduce(sermod.Or, sorted(self.factors)) if self.factors else None

            def __init__(self):
                self._segments: typing.Dict[frame.Table, Frame.Context.Tables.Segment] = collections.defaultdict(
                    self.Segment
                )

            def items(self) -> typing.ItemsView[frame.Table, 'Frame.Context.Tables.Segment']:
                """Get the key-value pairs of this mapping.

                Returns:
                    Key-value mapping items.
                """
                return self._segments.items()

            def __getitem__(self, table: frame.Table) -> 'Frame.Context.Tables.Segment':
                return self._segments[table]

            def select(self, *column: sermod.Column) -> None:
                """Extract fields from given list of columns and register them into segments of their relevant tables.

                Args:
                    *column: Columns to be to extracted and registered.
                """
                for field in sermod.Field.dissect(*column):
                    self[field.origin].fields.add(field)

            def filter(self, expression: sermod.Predicate) -> None:
                """Extract predicate factors from given expression and register them into segments of their relevant
                tables. Also register the whole expression using .select().

                Args:
                    expression: Expression to be extracted and registered.
                """
                self.select(expression)
                for table, factor in expression.factors.items():
                    self[table].factors.add(factor)

        def __init__(self):
            super().__init__()
            self.tables: Frame.Context.Tables = self.Tables()

    def __init__(self, sources: typing.Mapping[frame.Source, Source], columns: typing.Mapping[sermod.Column, Column]):
        super().__init__(sources)
        self._series: Frame.Series = self.Series(sources, columns)  # pylint: disable=abstract-class-instantiated

    @functools.lru_cache()
    def generate_column(self, column: sermod.Column) -> Column:
        """Generate target code for the generic column type.

        Args:
            column: Column instance

        Returns:
            Column in target code.
        """
        with self._series as visitor:
            column.accept(visitor)
            return visitor.fetch()

    @abc.abstractmethod
    def generate_reference(self, instance: Source, name: str) -> Source:
        """Generate reference code.

        Args:
            instance: Instance value already in target code.
            name: Reference name.

        Returns:
            Instance reference in target code.
        """

    @abc.abstractmethod
    def generate_join(
        self, left: Source, right: Source, condition: typing.Optional[Column], kind: frame.Join.Kind
    ) -> Source:
        """Generate target code for a join operation using the left/right terms, given condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns:
            Target code for the join operation.
        """

    @abc.abstractmethod
    def generate_set(self, left: Source, right: Source, kind: frame.Set.Kind) -> Source:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns:
            Target code for the set operation.
        """

    @abc.abstractmethod
    def generate_query(
        self,
        source: Source,
        columns: typing.Sequence[Column],
        where: typing.Optional[Column],
        groupby: typing.Sequence[Column],
        having: typing.Optional[Column],
        orderby: typing.Sequence[typing.Tuple[Column, sermod.Ordering.Direction]],
        rows: typing.Optional[frame.Rows],
    ) -> Source:
        """Generate query statement code.

        Args:
            source: Source already in target code.
            columns: Sequence of selected columns in target code.
            where: Where condition in target code.
            groupby: Sequence of grouping specifiers in target code.
            having: Having condition in target code.
            orderby: Ordering specifier in target code.
            rows: Limit spec tuple.

        Returns:
            Query in target code.
        """

    def generate_table(  # pylint: disable=no-self-use
        self,
        table: Source,
        columns: typing.Iterable[Column],  # pylint: disable=unused-argument
        predicate: typing.Optional[Column],  # pylint: disable=unused-argument
    ) -> Source:  # pylint: disable=unused-argument
        """Generate a target code for a table instance given its actual field requirements.

        Args:
            table: Table (already in target code based on the provided mapping) to be generated.
            columns: List of fields to be retrieved from the table (potentially subset of all available).
            predicate: Row filter to be possibly pushed down when retrieving the data from given table.

        Returns:
            Table target code potentially optimized based on field requirements.
        """
        return table

    def visit_table(self, origin: frame.Table) -> None:
        fields = [self.generate_column(f) for f in sorted(self.context.tables[origin].fields)]
        predicate = self.context.tables[origin].predicate
        if predicate is not None:
            predicate = self.generate_column(predicate)
        super().visit_table(origin)
        self.context.symbols.push(self.generate_table(self.resolve_source(origin), fields, predicate))

    def visit_reference(self, origin: frame.Reference) -> None:
        super().visit_reference(origin)
        self.context.symbols.push(self.generate_reference(self.context.symbols.pop(), origin.name))

    @bypass(Columnar.resolve_source)
    def visit_join(self, source: frame.Join) -> None:
        if source.condition:
            self.context.tables.filter(source.condition)
        super().visit_join(source)
        right = self.context.symbols.pop()
        left = self.context.symbols.pop()
        expression = self.generate_column(source.condition) if source.condition is not None else None
        self.context.symbols.push(self.generate_join(left, right, expression, source.kind))

    @bypass(Columnar.resolve_source)
    def visit_set(self, source: frame.Set) -> None:
        super().visit_set(source)
        right = self.context.symbols.pop()
        left = self.context.symbols.pop()
        self.context.symbols.push(self.generate_set(left, right, source.kind))

    @bypass(Columnar.resolve_source)
    def visit_query(self, source: frame.Query) -> None:
        with self:
            self.context.tables.select(*source.columns)
            if source.prefilter is not None:
                self.context.tables.filter(source.prefilter)
            if source.postfilter is not None:
                self.context.tables.select(source.postfilter)
            self.context.tables.select(*source.grouping)
            self.context.tables.select(*(c for c, _ in source.ordering))
            super().visit_query(source)
            columns = [self.generate_column(c) for c in source.columns]
            where = self.generate_column(source.prefilter) if source.prefilter is not None else None
            groupby = [self.generate_column(c) for c in source.grouping]
            having = self.generate_column(source.postfilter) if source.postfilter is not None else None
            orderby = [(self.generate_column(c), o) for c, o in source.ordering]
            query = self.generate_query(
                self.context.symbols.pop(), columns, where, groupby, having, orderby, source.rows
            )
        self.context.symbols.push(query)
