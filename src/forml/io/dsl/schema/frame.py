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
ETL schema types.
"""
import abc
import collections
import enum
import functools
import inspect
import itertools
import logging
import operator
import typing
from collections import abc as colabc

from forml.io import etl
from forml.io.dsl.schema import series

LOGGER = logging.getLogger(__name__)


class Ordering(collections.namedtuple('Ordering', 'column, direction')):
    """OrderBy spec.
    """
    @enum.unique
    class Direction(enum.Enum):
        """Ordering direction.
        """
        ASCENDING = 'ascending'
        DESCENDING = 'descending'

        def __call__(self, column: typing.Union[series.Element, 'Ordering']) -> 'Ordering':
            if isinstance(column, Ordering):
                column = column.column
            return Ordering(column, self)

    def __new__(cls, column: series.Element,
                direction: typing.Optional[typing.Union['Ordering.Direction', str]] = None):
        return super().__new__(cls, series.Element.ensure(column),
                               cls.Direction(direction) if direction else cls.Direction.ASCENDING)

    @classmethod
    def make(cls, specs: typing.Sequence[typing.Union[series.Element,
                                                      typing.Union['Ordering.Direction', str],
                                                      typing.Tuple[series.Element, typing.Union[
                                                          'Ordering.Direction', str]]]]) -> typing.Iterable['Ordering']:
        """Helper to generate orderings from given columns and directions.

        Args:
            specs: One or many columns or actual ordering instances.

        Returns: Sequence of ordering terms.
        """
        specs = itertools.zip_longest(specs, specs[1:])
        for column, direction in specs:
            if isinstance(column, series.Element):
                if isinstance(direction, (Ordering.Direction, str)):
                    yield Ordering.Direction(direction)(column)
                    next(specs)  # pylint: disable=stop-iteration-return
                else:
                    yield Ordering(column)
            elif isinstance(column, colabc.Sequence) and len(column) == 2:
                column, direction = column
                yield Ordering.Direction(direction)(column)
            else:
                raise ValueError('Expecting pair of column and direction')


class Rows(collections.namedtuple('Rows', 'count, offset')):
    """Row limit spec.
    """
    def __new__(cls, count: int, offset: int = 0):
        return super().__new__(cls, count, offset)


class Visitor(metaclass=abc.ABCMeta):
    """Schema visitor.
    """
    @abc.abstractmethod
    def visit_source(self, source: 'Source') -> None:
        """Generic source hook.

        Args:
            source: Source instance to be visited.
        """

    def visit_table(self, table: 'Table') -> None:
        """Generic source hook.

        Args:
            table: Source instance to be visited.
        """
        self.visit_source(table)

    def visit_join(self, source: 'Join') -> None:
        """Generic source hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)

    def visit_set(self, source: 'Set') -> None:
        """Generic source hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)

    def visit_query(self, source: 'Query') -> None:
        """Generic source hook.

        Args:
            source: Instance to be visited.
        """
        self.visit_source(source)


class Source(metaclass=abc.ABCMeta):
    """Source base class.
    """
    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence[series.Column]:
        """Get the list of columns representing this source.

        Returns: Sequence of columns.
        """

    @abc.abstractmethod
    def __hash__(self) -> int:
        """Custom hash function.

        Returns: Hashcode.
        """

    @abc.abstractmethod
    def __eq__(self, other) -> bool:
        """Custom equal implementation.

        Args:
            other: Operand to compare to.

        Returns: True if equal.
        """

    @functools.lru_cache()
    def __getattr__(self, name: str) -> series.Column:
        for column in self.columns:
            if column.name == name:
                return column
        raise AttributeError(f'Invalid column {name}')

    @abc.abstractmethod
    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """


class Join(collections.namedtuple('Join', 'left, right, condition, kind'), Source):
    """Source made of two join-combined subsources.
    """
    @enum.unique
    class Kind(enum.Enum):
        """Join type.
        """
        LEFT = 'left'
        RIGHT = 'right'
        INNER = 'inner'
        FULL = 'full'
        CROSS = 'cross'

    def __new__(cls, left: Source, right: Source, condition: series.Expression,
                kind: typing.Optional[typing.Union['Join.Kind', str]] = None):
        return super().__new__(cls, left, right, series.Logical.ensure(series.Element.ensure(condition)),
                               cls.Kind(kind) if kind else cls.Kind.LEFT)

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_join(self)


class Set(collections.namedtuple('Set', 'left, right, kind'), Source):
    """Source made of two set-combined subsources.
    """
    @enum.unique
    class Kind(enum.Enum):
        """Set type.
        """
        UNION = 'union'
        INTERSECTION = 'intersection'
        DIFFERENCE = 'difference'

    def __new__(cls, left: Source, right: Source, kind: 'Set.Kind'):
        return super().__new__(cls, left, right, kind)

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_set(self)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable sources.
    """
    # def reference(self, label: typing.Optional[str] = None) -> 'Reference':
    #     """Use a independent reference to this Source (ie for self-join conditions).
    #
    #     Args:
    #         label: Optional alias to be used for this reference.
    #
    #     Returns: New reference to this table.
    #     """
    #     return Reference(self, label)

    @property
    @abc.abstractmethod
    def query(self) -> 'Query':
        """Return query instance of this queryable.

        Returns: Query instance.
        """

    def select(self, *columns: series.Column) -> 'Query':
        """Specify the output columns to be provided.
        """
        return self.query.select(*columns)

    def where(self, condition: series.Expression) -> 'Query':
        """Add a row pre-filtering condition.
        """
        return self.query.where(condition)

    def having(self, condition: series.Expression) -> 'Query':
        """Add a row post-filtering condition.
        """
        return self.query.having(condition)

    def join(self, other: 'Queryable', condition: series.Expression,
             kind: typing.Optional[typing.Union[Join.Kind, str]] = None) -> 'Query':
        """Join with other source.
        """
        return self.query.join(other, condition, kind)

    def groupby(self, *columns: series.Element) -> 'Query':
        """Aggregating spec.
        """
        return self.query.groupby(*columns)

    def orderby(self, *columns: typing.Union[series.Element, typing.Union[
            Ordering.Direction, str], typing.Tuple[series.Element, typing.Union[Ordering.Direction, str]]]) -> 'Query':
        """Ordering spec.
        """
        return self.query.orderby(*columns)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        """Restrict the result rows by its max count with an optional offset.
        """
        return self.query.limit(count, offset)

    def union(self, other: 'Queryable') -> 'Query':
        """Set union with the other source.
        """
        return self.query.union(other)

    def intersection(self, other: 'Queryable') -> 'Query':
        """Set intersection with the other source.
        """
        return self.query.intersection(other)

    def difference(self, other: 'Queryable') -> 'Query':
        """Set difference with the other source.
        """
        return self.query.difference(other)


class Table(Queryable, tuple):
    """Table based source.

    This type can be used either as metaclass or as a base class to inherit from.
    """
    class Schema(type):
        """Meta class for schema type ensuring consistent hashing.
        """
        def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]):
            existing = {s[k].name or k for s in bases if isinstance(s, Table.Schema) for k in s}
            for key, field in namespace.items():
                if not isinstance(field, etl.Field):
                    continue
                new = field.name or key
                if new in existing:
                    raise TypeError(f'Colliding field name {new} in schema {name}')
                existing.add(new)
            if bases and not existing:
                raise TypeError(f'No fields defined for schema {name}')
            return super().__new__(mcs, name, bases, namespace)

        def __hash__(cls):
            return hash(cls.__module__) ^ hash(cls.__qualname__)

        def __eq__(cls, other):
            return hash(cls) == hash(other)

        def __getitem__(cls, key: str) -> 'etl.Field':
            return getattr(cls, key)

        def __iter__(cls) -> typing.Iterator[str]:
            return iter(k for k, _ in cls.items())  # pylint: disable=no-value-for-parameter

        def items(cls) -> typing.Iterator[typing.Tuple[str, 'etl.Field']]:
            """Get the schema items as pairs of key and Field instance.

            Returns: Iterator of key-Field pairs.
            """
            return iter((k, f) for c in reversed(inspect.getmro(cls))
                        for k, f in c.__dict__.items() if isinstance(f, etl.Field))

    __schema__: typing.Type['etl.Schema'] = property(operator.itemgetter(0))

    def __new__(mcs, schema: typing.Union[str, typing.Type['etl.Schema']],  # pylint: disable=bad-classmethod-argument
                bases: typing.Optional[typing.Tuple[typing.Type]] = None,
                namespace: typing.Optional[typing.Dict[str, typing.Any]] = None):
        if issubclass(mcs, Table):  # used as metaclass
            if bases:
                bases = (bases[0].__schema__, )
            schema = mcs.Schema(schema, bases, namespace)
        else:
            if bases or namespace:
                raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, [schema])  # used as constructor

    def __hash__(self):
        return hash(self.__schema__)

    def __eq__(self, other):
        return isinstance(other, Table) and other.__schema__ == self.__schema__

    @functools.lru_cache()
    def __getattr__(self, name: str) -> 'series.Field':
        try:
            field: 'etl.Field' = self.__schema__[name]
        except KeyError:
            return super().__getattr__(name)
        return series.Field(self, field.name or name, field.kind)

    @property
    def query(self) -> 'Query':
        return Query(self)

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Field']:
        return tuple(series.Field(self, f.name or k, f.kind) for k, f in self.__schema__.items())

    def accept(self, visitor: Visitor) -> None:
        visitor.visit_table(self)


class Query(collections.namedtuple('Query', 'source, selection, prefilter, grouping, postfilter, ordering, rows'),
            Queryable):
    """Generic source descriptor.
    """
    def __new__(cls, source: Source,
                selection: typing.Optional[typing.Iterable[series.Column]] = None,
                prefilter: typing.Optional[series.Expression] = None,
                grouping: typing.Optional[typing.Iterable[series.Element]] = None,
                postfilter: typing.Optional[series.Expression] = None,
                ordering: typing.Optional[typing.Sequence[typing.Union[series.Element,
                                                                       typing.Union[Ordering.Direction, str],
                                                                       typing.Tuple[series.Element, typing.Union[
                                                                           Ordering.Direction, str]]]]] = None,
                rows: typing.Optional[Rows] = None):

        if selection and series.Field.disect(*selection) - series.Field.disect(*source.columns):
            raise ValueError(f'Selection ({selection}) is not a subset of source columns ({source.columns})')
        if prefilter is not None:
            prefilter = series.Logical.ensure(series.Element.ensure(prefilter))
        if postfilter is not None:
            postfilter = series.Logical.ensure(series.Element.ensure(postfilter))
        return super().__new__(cls, source, tuple(selection or []), prefilter,
                               tuple(series.Element.ensure(g) for g in grouping or []), postfilter,
                               tuple(Ordering.make(ordering or [])), rows)

    @property
    def query(self) -> 'Query':
        return self

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.selection if self.selection else self.source.columns

    def accept(self, visitor: Visitor) -> None:
        self.source.accept(visitor)
        visitor.visit_query(self)

    def select(self, *columns: series.Column) -> 'Query':
        return Query(self.source, columns, self.prefilter, self.grouping, self.postfilter, self.ordering, self.rows)

    def where(self, condition: series.Expression) -> 'Query':
        if self.prefilter is not None:
            condition &= self.prefilter
        return Query(self.source, self.selection, condition, self.grouping, self.postfilter, self.ordering, self.rows)

    def having(self, condition: series.Expression) -> 'Query':
        if self.postfilter is not None:
            condition &= self.postfilter
        return Query(self.source, self.selection, self.prefilter, self.grouping, condition, self.ordering, self.rows)

    def join(self, other: Queryable, condition: series.Expression,
             kind: typing.Optional[typing.Union[Join.Kind, str]] = None) -> 'Query':
        return Query(Join(self.source, other, condition, kind), self.selection, self.prefilter, self.grouping,
                     self.postfilter, self.ordering, self.rows)

    def groupby(self, *columns: series.Element) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, columns, self.postfilter, self.ordering, self.rows)

    def orderby(self, *columns: typing.Union[series.Element, typing.Union['Ordering.Direction', str], typing.Tuple[
            series.Element, typing.Union['Ordering.Direction', str]]]) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, columns, self.rows)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, self.ordering,
                     Rows(count, offset))
