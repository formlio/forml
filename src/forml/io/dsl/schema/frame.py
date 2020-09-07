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
import random
import string
import typing
from collections import abc as colabc

from forml.io import etl
from forml.io.dsl.schema import series, visit

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


class Rows(typing.NamedTuple):
    """Row limit spec.
    """
    count: int
    offset: int = 0


class Source(tuple, metaclass=abc.ABCMeta):
    """Source base class.
    """
    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence[series.Column]:
        """Get the list of columns representing this source.

        Returns: Sequence of columns.
        """

    @property
    @functools.lru_cache()
    def schema(self) -> typing.Type['etl.Schema']:
        """Get the schema type for this source.

        Returns: Schema type.
        """
        return Table.Schema(f'{self.__class__.__name__}Schema', (etl.Schema.schema, ), {
            (c.name or f'_{i}'): etl.Field(c.kind, c.name) for i, c in enumerate(self.columns)})

    def __getattr__(self, name: str) -> series.Column:
        try:
            return self[name]
        except KeyError as err:
            raise AttributeError(f'Invalid column {name}') from err

    @functools.lru_cache()
    def __getitem__(self, name: typing.Union[int, str]) -> typing.Any:
        try:
            return super().__getitem__(name)
        except (TypeError, IndexError) as err:
            for (key, field), column in zip(self.schema.items(), self.columns):
                if name in {key, field.name}:
                    return column
            raise KeyError(f'Invalid column {name}') from err

    @abc.abstractmethod
    def accept(self, visitor: visit.Source) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """


class Join(Source):
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

    left: 'Tangible' = property(operator.itemgetter(0))
    right: 'Tangible' = property(operator.itemgetter(1))
    condition: series.Expression = property(operator.itemgetter(2))
    kind: 'Join.Kind' = property(operator.itemgetter(3))

    def __new__(cls, left: 'Tangible', right: 'Tangible', condition: series.Expression,
                kind: typing.Optional[typing.Union['Join.Kind', str]] = None):
        return super().__new__(cls, [left, right, series.Logical.ensure(series.Element.ensure(condition)),
                               cls.Kind(kind) if kind else cls.Kind.LEFT])

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    def accept(self, visitor: visit.Source) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_join(self)


class Set(Source):
    """Source made of two set-combined subsources.
    """
    @enum.unique
    class Kind(enum.Enum):
        """Set type.
        """
        UNION = 'union'
        INTERSECTION = 'intersection'
        DIFFERENCE = 'difference'

    left: Source = property(operator.itemgetter(0))
    right: Source = property(operator.itemgetter(1))
    kind: 'Set.Kind' = property(operator.itemgetter(2))

    def __new__(cls, left: Source, right: Source, kind: 'Set.Kind'):
        return super().__new__(cls, [left, right, kind])

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    def accept(self, visitor: visit.Source) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_set(self)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable sources.
    """
    @property
    def query(self) -> 'Query':
        """Return query instance of this queryable.

        Returns: Query instance.
        """
        return Query(self)

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        """Use a independent reference to this Source (ie for self-join conditions).

        Args:
            name: Optional alias to be used for this reference.

        Returns: New reference to this table.
        """
        return Reference(self, name)

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

    def join(self, other: 'Tangible', condition: series.Expression,
             kind: typing.Optional[typing.Union[Join.Kind, str]] = None) -> 'Query':
        """Join with other tangible.
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


class Tangible(Queryable, metaclass=abc.ABCMeta):
    """Tangible is a queryable that can be referenced by some identifier (rather than just a statement itself).

    It's columns are represented using series.Field.
    """
    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence[series.Field]:
        """Tangible columns are instances of series.Field.

        Returns: Sequence of series.Field instances.
        """


class Reference(Tangible):
    """Reference is a wrapper around a queryable that associates it with a (possibly random) name.
    """
    _NAMELEN: int = 8
    instance: Queryable = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, instance: Queryable, name: typing.Optional[str] = None):
        if not name:
            name = ''.join(random.choice(string.ascii_lowercase) for _ in range(cls._NAMELEN))
        return super().__new__(cls, [instance, name])

    @property
    def query(self) -> 'Query':
        return self.instance.query

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Field]:
        return tuple(series.Field(self, c.name) for c in self.instance.columns)

    @property
    def schema(self) -> typing.Type['etl.Schema']:
        return self.instance.schema

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        return Reference(self.instance, name)

    def accept(self, visitor: visit.Source) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        self.instance.accept(visitor)
        visitor.visit_reference(self)


class Table(Tangible):
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

        @functools.lru_cache()
        def __getitem__(cls, name: str) -> 'etl.Field':
            for key, field in cls.items():  # pylint: disable=no-value-for-parameter
                if name in {key, field.name}:
                    return field
            raise AttributeError(f'Unknown field {name}')

        def __iter__(cls) -> typing.Iterator[str]:
            return iter(k for k, _ in cls.items())  # pylint: disable=no-value-for-parameter

        def items(cls) -> typing.Iterator[typing.Tuple[str, 'etl.Field']]:
            """Get the schema items as pairs of key and Field instance.

            Returns: Iterator of key-Field pairs.
            """
            return iter((k, f) for c in reversed(inspect.getmro(cls))
                        for k, f in c.__dict__.items() if isinstance(f, etl.Field))

    schema: typing.Type['etl.Schema'] = property(operator.itemgetter(0))

    def __new__(mcs, schema: typing.Union[str, typing.Type['etl.Schema']],  # pylint: disable=bad-classmethod-argument
                bases: typing.Optional[typing.Tuple[typing.Type]] = None,
                namespace: typing.Optional[typing.Dict[str, typing.Any]] = None):
        if issubclass(mcs, Table):  # used as metaclass
            if bases:
                bases = (bases[0].schema, )
            schema = mcs.Schema(schema, bases, namespace)
        else:
            if bases or namespace:
                raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, [schema])  # used as constructor

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Field]:
        return tuple(series.Field(self, f.name or k) for k, f in self.schema.items())

    def accept(self, visitor: visit.Source) -> None:
        visitor.visit_table(self)


class Query(Queryable):
    """Generic source descriptor.
    """
    source: Source = property(operator.itemgetter(0))
    selection: typing.Tuple[series.Column] = property(operator.itemgetter(1))
    prefilter: series.Expression = property(operator.itemgetter(2))
    grouping: typing.Tuple[series.Element] = property(operator.itemgetter(3))
    postfilter: series.Expression = property(operator.itemgetter(4))
    ordering: typing.Tuple[Ordering] = property(operator.itemgetter(5))
    rows: Rows = property(operator.itemgetter(6))

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

        if selection and series.Field.dissect(*selection) - series.Field.dissect(*source.columns):
            raise ValueError(f'Selection ({selection}) is not a subset of source columns ({source.columns})')
        if prefilter is not None:
            prefilter = series.Logical.ensure(series.Element.ensure(prefilter))
        if postfilter is not None:
            postfilter = series.Logical.ensure(series.Element.ensure(postfilter))
        return super().__new__(cls, [source, tuple(selection or []), prefilter,
                               tuple(series.Element.ensure(g) for g in grouping or []), postfilter,
                               tuple(Ordering.make(ordering or [])), rows])

    @property
    def query(self) -> 'Query':
        return self

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.selection if self.selection else self.source.columns

    def accept(self, visitor: visit.Source) -> None:
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
