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

from forml.io import dsl
from forml.io.dsl import function

LOGGER = logging.getLogger(__name__)

Source = typing.TypeVar('Source')
Feature = typing.TypeVar('Feature')
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
                self._stack: list[Symbol] = []

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

        class Tables:
            """Container for segments of all tables."""

            class Segment(collections.namedtuple('Segment', 'fields, factors')):
                """Frame segment specification as a list of features (vertical) and row predicates (horizontal)."""

                def __new__(cls):
                    return super().__new__(cls, set(), set())

                @property
                def predicate(self) -> typing.Optional[dsl.Predicate]:
                    """Combine the factors into single predicate.

                    Returns:
                        Predicate expression.
                    """
                    return functools.reduce(function.Or, sorted(self.factors)) if self.factors else None

            def __init__(self):
                self._segments: dict[dsl.Table, Container.Context.Tables.Segment] = collections.defaultdict(
                    self.Segment
                )

            def items(self) -> typing.ItemsView[dsl.Table, 'Container.Context.Tables.Segment']:
                """Get the key-value pairs of this mapping.

                Returns:
                    Key-value mapping items.
                """
                return self._segments.items()

            def __getitem__(self, table: dsl.Table) -> 'Container.Context.Tables.Segment':
                return self._segments[table]

            def select(self, *feature: dsl.Feature) -> None:
                """Extract fields from given list of features and register them into segments of their relevant tables.

                Args:
                    *feature: Features to be to extracted and registered.
                """
                for field in dsl.Column.dissect(*feature):
                    self[field.origin].fields.add(field)

            def filter(self, expression: dsl.Predicate) -> None:
                """Extract predicate factors from given expression and register them into segments of their relevant
                tables. Also register the whole expression using .select().

                Args:
                    expression: Expression to be extracted and registered.
                """
                self.select(expression)
                for table, factor in expression.factors.items():
                    self[table].factors.add(factor)

        def __init__(self):
            self.symbols: Container.Context.Symbols = self.Symbols()
            self.tables: Container.Context.Tables = self.Tables()
            self.origins: dict[dsl.Origin, Source] = {}

        @property
        def dirty(self) -> bool:
            """Check the context is safe to be closed.

            Returns:
                True if not safe for closing.
            """
            return bool(self.symbols)

    def __init__(self):
        self._context: typing.Optional[Container.Context] = None
        self._stack: list[Container.Context] = []

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
        symbol = self._context.symbols.pop()
        if self._context.dirty:
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
            except dsl.UnprovisionedError:
                pass
            else:
                old = self.context.symbols.pop()
                LOGGER.debug('Overriding result for %s (%s -> %s)', subject, old, new)
                self.context.symbols.push(new)

        return wrapped

    return decorator


class Visitor(
    typing.Generic[Source, Feature],
    Container[typing.Union[Source, Feature]],
    dsl.Source.Visitor,
    dsl.Feature.Visitor,
    metaclass=abc.ABCMeta,
):
    """Frame source parser."""

    def __init__(self, sources: typing.Mapping[dsl.Source, Source], features: typing.Mapping[dsl.Feature, Feature]):
        super().__init__()
        self._sources: typing.Mapping[dsl.Source, Source] = types.MappingProxyType(sources)
        self._features: typing.Mapping[dsl.Feature, Feature] = types.MappingProxyType(features)

    def resolve_feature(self, feature: dsl.Feature) -> Feature:
        """Get a custom target code for a feature value.

        Args:
            feature: Feature instance.

        Returns:
            Feature in target code representation.
        """
        try:
            return self._features[feature]
        except KeyError as err:
            raise dsl.UnprovisionedError(f'Unknown mapping for feature {feature}') from err

    @functools.cache
    def generate_feature(self, feature: dsl.Feature) -> Feature:
        """Generate target code for the generic feature type.

        Args:
            feature: Feature instance

        Returns:
            Feature in target code.
        """
        feature.accept(self)
        return self.context.symbols.pop()

    @abc.abstractmethod
    def generate_element(self, origin: Source, element: Feature) -> Feature:
        """Generate an element code.

        Args:
            origin: Origin value already in target code.
            element: Element symbol to be used for given feature.

        Returns:
            Element in target code.
        """

    @abc.abstractmethod
    def generate_alias(self, feature: Feature, alias: str) -> Feature:
        """Generate feature alias code.

        Args:
            feature: Feature value already in target code.
            alias: Alias to be used for given feature.

        Returns:
            Aliased feature in target code.
        """

    @abc.abstractmethod
    def generate_literal(self, value: typing.Any, kind: dsl.Any) -> Feature:
        """Generate target code for a literal value.

        Args:
            value: Literal value instance.
            kind: Literal value type.

        Returns:
            Literal in target code representation.
        """

    @abc.abstractmethod
    def generate_expression(self, expression: type[dsl.Expression], arguments: typing.Sequence[typing.Any]) -> Feature:
        """Generate target code for an expression of given arguments.

        Args:
            expression: Operator or function implementing the expression.
            arguments: Expression arguments.

        Returns:
            Expression in target code representation.
        """

    def visit_aliased(self, feature: dsl.Aliased) -> None:
        super().visit_aliased(feature)
        self.context.symbols.push(self.generate_alias(self.context.symbols.pop(), feature.name))

    def visit_literal(self, feature: dsl.Literal) -> None:
        super().visit_literal(feature)
        self.context.symbols.push(self.generate_literal(feature.value, feature.kind))

    def visit_element(self, feature: dsl.Element) -> None:
        super().visit_element(feature)
        self.context.symbols.push(
            self.generate_element(self.context.origins[feature.origin], self.resolve_feature(feature))
        )

    @bypass(resolve_feature)
    def visit_expression(self, feature: dsl.Expression) -> None:
        super().visit_expression(feature)
        arguments = tuple(
            reversed([self.context.symbols.pop() if isinstance(c, dsl.Feature) else c for c in reversed(feature)])
        )
        self.context.symbols.push(self.generate_expression(feature.__class__, arguments))

    @bypass(resolve_feature)
    def visit_window(self, feature: dsl.Window) -> typing.ContextManager[None]:
        raise RuntimeError('Window functions not yet supported')

    def resolve_source(self, source: dsl.Source) -> Source:
        """Get a custom target code for a source type.

        Args:
            source: Source instance.

        Returns:
            Target code for the source instance.
        """
        try:
            return self._sources[source]
        except KeyError as err:
            raise dsl.UnprovisionedError(f'Unknown mapping for source {source}') from err

    def generate_table(  # pylint: disable=no-self-use
        self,
        table: Source,
        features: typing.Iterable[Feature],  # pylint: disable=unused-argument
        predicate: typing.Optional[Feature],  # pylint: disable=unused-argument
    ) -> Source:  # pylint: disable=unused-argument
        """Generate a target code for a table instance given its actual field requirements.

        Args:
            table: Table (already in target code based on the provided mapping) to be generated.
            features: List of fields to be retrieved from the table (potentially subset of all available).
            predicate: Row filter to be possibly pushed down when retrieving the data from given table.

        Returns:
            Table target code potentially optimized based on field requirements.
        """
        return table

    @abc.abstractmethod
    def generate_reference(self, instance: Source, name: str) -> tuple[Source, Source]:
        """Generate reference code.

        Args:
            instance: Instance value already in target code.
            name: Reference name.

        Returns:
            Tuple of referenced origin and the bare reference handle both in target code.
        """

    @abc.abstractmethod
    def generate_join(
        self, left: Source, right: Source, condition: typing.Optional[Feature], kind: dsl.Join.Kind
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
    def generate_set(self, left: Source, right: Source, kind: dsl.Set.Kind) -> Source:
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
        features: typing.Sequence[Feature],
        where: typing.Optional[Feature],
        groupby: typing.Sequence[Feature],
        having: typing.Optional[Feature],
        orderby: typing.Sequence[tuple[Feature, dsl.Ordering.Direction]],
        rows: typing.Optional[dsl.Rows],
    ) -> Source:
        """Generate query statement code.

        Args:
            source: Source already in target code.
            features: Sequence of selected features in target code.
            where: Where condition in target code.
            groupby: Sequence of grouping specifiers in target code.
            having: Having condition in target code.
            orderby: Ordering specifier in target code.
            rows: Limit spec tuple.

        Returns:
            Query in target code.
        """

    def visit_table(self, source: dsl.Table) -> None:
        self.context.origins[source] = origin = self.resolve_source(source)
        features = [self.generate_feature(f) for f in sorted(self.context.tables[source].fields)]
        predicate = self.context.tables[source].predicate
        if predicate is not None:
            predicate = self.generate_feature(predicate)
        super().visit_table(source)
        self.context.symbols.push(self.generate_table(origin, features, predicate))

    def visit_reference(self, source: dsl.Reference) -> None:
        super().visit_reference(source)
        origin, handle = self.generate_reference(self.context.symbols.pop(), source.name)
        self.context.origins[source] = handle
        self.context.symbols.push(origin)

    @bypass(resolve_source)
    def visit_join(self, source: dsl.Join) -> None:
        if source.condition:
            self.context.tables.filter(source.condition)
        super().visit_join(source)
        right = self.context.symbols.pop()
        left = self.context.symbols.pop()
        expression = self.generate_feature(source.condition) if source.condition is not None else None
        self.context.symbols.push(self.generate_join(left, right, expression, source.kind))

    @bypass(resolve_source)
    def visit_set(self, source: dsl.Set) -> None:
        super().visit_set(source)
        right = self.context.symbols.pop()
        left = self.context.symbols.pop()
        self.context.symbols.push(self.generate_set(left, right, source.kind))

    @bypass(resolve_source)
    def visit_query(self, source: dsl.Query) -> None:
        with self:
            self.context.tables.select(*source.features)
            if source.prefilter is not None:
                self.context.tables.filter(source.prefilter)
            if source.postfilter is not None:
                self.context.tables.select(source.postfilter)
            self.context.tables.select(*source.grouping)
            self.context.tables.select(*(c for c, _ in source.ordering))
            super().visit_query(source)
            features = [self.generate_feature(c) for c in source.features]
            where = self.generate_feature(source.prefilter) if source.prefilter is not None else None
            groupby = [self.generate_feature(c) for c in source.grouping]
            having = self.generate_feature(source.postfilter) if source.postfilter is not None else None
            orderby = [(self.generate_feature(c), o) for c, o in source.ordering]
            query = self.generate_query(
                self.context.symbols.pop(), features, where, groupby, having, orderby, source.rows
            )
        self.context.symbols.push(query)
