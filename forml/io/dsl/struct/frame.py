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
import enum
import functools
import inspect
import logging
import operator
import random
import string
import typing

from forml.io.dsl import error, struct
from forml.io.dsl.struct import series, visit

LOGGER = logging.getLogger(__name__)


class Rows(typing.NamedTuple):
    """Row limit spec."""

    count: int
    offset: int = 0

    def __repr__(self):
        return f'{self.offset}:{self.count}'


class Source(tuple, metaclass=abc.ABCMeta):
    """Source base class."""

    def __new__(cls, *args):
        return super().__new__(cls, args)

    def __getnewargs__(self):
        return tuple(self)

    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(a) for a in self)})'

    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence['series.Column']:
        """Get the list of columns supplied by this source.

        Returns:
            Sequence of supplying columns.
        """

    @property
    @functools.lru_cache()
    def schema(self) -> typing.Type['struct.Schema']:
        """Get the schema type for this source.

        Returns:
            Schema type.
        """
        return Table.Schema(
            f'{self.__class__.__name__}Schema',
            (struct.Schema.schema,),
            {(c.name or f'_{i}'): struct.Field(c.kind, c.name) for i, c in enumerate(self.columns)},
        )

    def __getattr__(self, name: str) -> 'series.Column':
        try:
            return self[name]
        except KeyError as err:
            raise AttributeError(f'Invalid column {name}') from err

    @functools.lru_cache()
    def __getitem__(self, name: typing.Union[int, str]) -> typing.Any:
        try:
            return super().__getitem__(name)
        except (TypeError, IndexError) as err:
            for key, column in zip(self.schema, self.columns):
                if name in {key, self.schema[key].name}:
                    return column
            raise KeyError(f'Invalid column {name}') from err

    @abc.abstractmethod
    def accept(self, visitor: visit.Frame) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """


class Join(Source):
    """Source made of two join-combined subsources."""

    @enum.unique
    class Kind(enum.Enum):
        """Join type."""

        INNER = 'inner'
        LEFT = 'left'
        RIGHT = 'right'
        FULL = 'full'
        CROSS = 'cross'

        def __repr__(self):
            return f'<{self.value}-join>'

    left: 'Origin' = property(operator.itemgetter(0))
    right: 'Origin' = property(operator.itemgetter(1))
    condition: typing.Optional['series.Expression'] = property(operator.itemgetter(2))
    kind: 'Join.Kind' = property(operator.itemgetter(3))

    def __new__(
        cls,
        left: 'Origin',
        right: 'Origin',
        condition: typing.Optional['series.Expression'] = None,
        kind: typing.Optional[typing.Union['Join.Kind', str]] = None,
    ):
        kind = cls.Kind(kind) if kind else cls.Kind.INNER if condition is not None else cls.Kind.CROSS
        if condition is not None:
            if kind is cls.Kind.CROSS:
                raise error.Syntax('Illegal use of condition for cross-join')
            condition = series.Cumulative.ensure_notin(series.Predicate.ensure_is(condition))
            if not series.Element.dissect(condition).issubset(series.Element.dissect(*left.columns, *right.columns)):
                raise error.Syntax(f'({condition}) not a subset of source columns ({left.columns}, {right.columns})')
        return super().__new__(cls, left, right, condition, kind)

    def __repr__(self):
        return f'{repr(self.left)}{repr(self.kind)}{repr(self.right)}'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Column']:
        return self.left.columns + self.right.columns

    def accept(self, visitor: visit.Frame) -> None:
        visitor.visit_join(self)


class Set(Source):
    """Source made of two set-combined subsources."""

    @enum.unique
    class Kind(enum.Enum):
        """Set type."""

        UNION = 'union'
        INTERSECTION = 'intersection'
        DIFFERENCE = 'difference'

    left: Source = property(operator.itemgetter(0))
    right: Source = property(operator.itemgetter(1))
    kind: 'Set.Kind' = property(operator.itemgetter(2))

    def __new__(cls, left: Source, right: Source, kind: 'Set.Kind'):
        return super().__new__(cls, left, right, kind)

    def __repr__(self):
        return f'{repr(self.left)} {self.kind.value} {repr(self.right)}'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Column']:
        return self.left.columns + self.right.columns

    def accept(self, visitor: visit.Frame) -> None:
        visitor.visit_set(self)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable sources."""

    @property
    def query(self) -> 'Query':
        """Return query instance of this queryable.

        Returns:
            Query instance.
        """
        return Query(self)

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        """Use a independent reference to this Source (ie for self-join conditions).

        Args:
            name: Optional alias to be used for this reference.

        Returns:
            New reference to this table.
        """
        return Reference(self, name)

    @property
    def instance(self) -> 'Source':
        """Return the source instance - which apart from the Reference type is the source itself.

        Returns:
            Source instance.
        """
        return self

    def select(self, *columns: 'series.Column') -> 'Query':
        """Specify the output columns to be provided (projection).

        Args:
            columns: Sequence of column expressions.

        Returns:
            Query instance.
        """
        return self.query.select(*columns)

    def where(self, condition: 'series.Expression') -> 'Query':
        """Add a row-filtering condition that's evaluated before any aggregations.

        Repeated calls to ``.where`` combine all the conditions (logical AND).

        Args:
            condition: Boolean column expression.

        Returns:
            Query instance.
        """
        return self.query.where(condition)

    def having(self, condition: 'series.Expression') -> 'Query':
        """Add a row-filtering condition that's applied to the evaluated aggregations.

        Repeated calls to ``.having`` combine all the conditions (logical AND).

        Args:
            condition: Boolean column expression.

        Returns:
            Query instance.
        """
        return self.query.having(condition)

    def join(
        self,
        other: 'Origin',
        condition: typing.Optional['series.Expression'] = None,
        kind: typing.Optional[typing.Union[Join.Kind, str]] = None,
    ) -> 'Query':
        """Join with another datasource.

        Args:
            other: Source to join with.
            condition: Column expression as the join condition.
            kind: Type of the join operation (INNER, LEFT, RIGHT, FULL CROSS).

        Returns:
            Query instance.
        """
        return self.query.join(other, condition, kind)

    def groupby(self, *columns: 'series.Operable') -> 'Query':
        """Aggregation specifiers.

        Args:
            columns: Sequence of column expressions.

        Returns:
            Query instance.
        """
        return self.query.groupby(*columns)

    def orderby(
        self,
        *columns: typing.Union[
            'series.Operable',
            typing.Union['series.Ordering.Direction', str],
            typing.Tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
        ],
    ) -> 'Query':
        """Ordering specifiers.

        Args:
            *columns: Sequence of column expressions and direction tuples.

        Returns:
            Query instance.
        """
        return self.query.orderby(*columns)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        """Restrict the result rows by its max count with an optional offset.

        Args:
            count: Number of rows to return.
            offset: Skip the given number of rows.

        Returns:
            Query instance.
        """
        return self.query.limit(count, offset)

    def union(self, other: 'Queryable') -> 'Query':
        """Set union with the other source.

        Args:
            other: Query to union with.

        Returns:
            Query instance.
        """
        return self.query.union(other)

    def intersection(self, other: 'Queryable') -> 'Query':
        """Set intersection with the other source.

        Args:
            other: Query to intersect with.

        Returns:
            Query instance.
        """
        return self.query.intersection(other)

    def difference(self, other: 'Queryable') -> 'Query':
        """Set difference with the other source.

        Args:
            other: Query to difference with.

        Returns:
            Query instance.
        """
        return self.query.difference(other)


class Origin(Queryable, metaclass=abc.ABCMeta):
    """Origin is a queryable that can be referenced by some identifier (rather than just a statement itself).

    It's columns are represented using series.Element.
    """

    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence['series.Element']:
        """Tangible columns are instances of series.Element.

        Returns:
            Sequence of series.Field instances.
        """


class Reference(Origin):
    """Reference is a wrapper around a queryable that associates it with a (possibly random) name."""

    _NAMELEN: int = 8
    instance: Queryable = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, instance: Queryable, name: typing.Optional[str] = None):
        if not name:
            name = ''.join(random.choice(string.ascii_lowercase) for _ in range(cls._NAMELEN))
        return super().__new__(cls, instance, name)

    def __repr__(self):
        return f'{self.name}=[{repr(self.instance)}]'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Element']:
        return tuple(series.Element(self, c.name) for c in self.instance.columns)

    @property
    def schema(self) -> typing.Type['struct.Schema']:
        return self.instance.schema

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        return Reference(self.instance, name)

    def accept(self, visitor: visit.Columnar) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_reference(self)


class Table(Origin):
    """Table based source.

    This type can be used either as metaclass or as a base class to inherit from.
    """

    class Schema(type):
        """Meta class for schema type ensuring consistent hashing."""

        def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]):
            existing = {s[k].name: k for s in bases if isinstance(s, Table.Schema) for k in s}
            for key, field in namespace.items():
                if not isinstance(field, struct.Field):
                    continue
                if not field.name:
                    namespace[key] = field = field.renamed(key)  # to normalize so that hash/eq is consistent
                if field.name in existing and field.name != existing[field.name]:
                    raise error.Syntax(f'Colliding field name {field.name} in schema {name}')
                existing[field.name] = key
            return super().__new__(mcs, name, bases, namespace)

        def __hash__(cls):
            # pylint: disable=not-an-iterable
            return functools.reduce(operator.xor, (hash(getattr(cls, k)) for k in cls), 0)

        def __eq__(cls, other: typing.Type['struct.Schema']):
            return len(cls) == len(other) and all(getattr(cls, c) == getattr(other, o) for c, o in zip(cls, other))

        def __len__(cls):
            return sum(1 for _ in cls)  # pylint: disable=not-an-iterable

        def __repr__(cls):
            return f'{cls.__module__}:{cls.__qualname__}'

        @functools.lru_cache()
        def __getitem__(cls, name: str) -> 'struct.Field':
            for key in cls:  # pylint: disable=not-an-iterable
                field = getattr(cls, key)
                if name in {key, field.name}:
                    return field
            raise AttributeError(f'Unknown field {name}')

        def __iter__(cls) -> typing.Iterator[str]:
            seen = set()  # fields overridden by inheritance need to appear in original position
            return iter(
                k
                for c in reversed(inspect.getmro(cls))
                for k, f in c.__dict__.items()
                if isinstance(f, struct.Field) and k not in seen and not seen.add(k)
            )

    schema: typing.Type['struct.Schema'] = property(operator.itemgetter(0))

    def __new__(  # pylint: disable=bad-classmethod-argument
        mcs,
        schema: typing.Union[str, typing.Type['struct.Schema']],
        bases: typing.Optional[typing.Tuple[typing.Type]] = None,
        namespace: typing.Optional[typing.Dict[str, typing.Any]] = None,
    ):
        if isinstance(schema, str):  # used as metaclass
            if bases:
                bases = (bases[0].schema,)
            schema = mcs.Schema(schema, bases, namespace)
        else:
            if bases or namespace:
                raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, schema)  # used as constructor

    def __repr__(self):
        return self.schema.__name__

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Field']:
        return tuple(series.Field(self, self.schema[k].name or k) for k in self.schema)

    def accept(self, visitor: visit.Columnar) -> None:
        visitor.visit_table(self)


class Query(Queryable):
    """Generic source descriptor."""

    source: Source = property(operator.itemgetter(0))
    selection: typing.Tuple['series.Column'] = property(operator.itemgetter(1))
    prefilter: typing.Optional['series.Expression'] = property(operator.itemgetter(2))
    grouping: typing.Tuple['series.Operable'] = property(operator.itemgetter(3))
    postfilter: typing.Optional['series.Expression'] = property(operator.itemgetter(4))
    ordering: typing.Tuple['series.Ordering'] = property(operator.itemgetter(5))
    rows: typing.Optional[Rows] = property(operator.itemgetter(6))

    def __new__(
        cls,
        source: Source,
        selection: typing.Optional[typing.Iterable['series.Column']] = None,
        prefilter: typing.Optional['series.Expression'] = None,
        grouping: typing.Optional[typing.Iterable['series.Operable']] = None,
        postfilter: typing.Optional['series.Expression'] = None,
        ordering: typing.Optional[
            typing.Sequence[
                typing.Union[
                    'series.Operable',
                    typing.Union['series.Ordering.Direction', str],
                    typing.Tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
                ]
            ]
        ] = None,
        rows: typing.Optional[Rows] = None,
    ):
        def ensure_subset(*columns: 'series.Column') -> typing.Sequence['series.Column']:
            """Ensure the provided columns is a valid subset of the available source columns.

            Args:
                *columns: List of columns to validate.

            Returns:
                Original list of columns if all valid.
            """
            if not series.Element.dissect(*columns).issubset(superset):
                raise error.Syntax(f'{columns} not a subset of source columns: {superset}')
            return columns

        superset = series.Element.dissect(*source.columns)
        selection = tuple(ensure_subset(*(series.Column.ensure_is(c) for c in selection or [])))
        if prefilter is not None:
            prefilter = series.Cumulative.ensure_notin(
                series.Predicate.ensure_is(*ensure_subset(series.Operable.ensure_is(prefilter)))
            )
        if grouping:
            grouping = ensure_subset(*(series.Cumulative.ensure_notin(series.Operable.ensure_is(g)) for g in grouping))
            for aggregate in {c.operable for c in selection or source.columns}.difference(grouping):
                series.Aggregate.ensure_in(aggregate)
        if postfilter is not None:
            postfilter = series.Window.ensure_notin(
                series.Predicate.ensure_is(*ensure_subset(series.Operable.ensure_is(postfilter)))
            )
        ordering = tuple(series.Ordering.make(ordering or []))
        ensure_subset(*(o.column for o in ordering))
        return super().__new__(cls, source, selection, prefilter, tuple(grouping or []), postfilter, ordering, rows)

    def __repr__(self):
        value = repr(self.source)
        if self.selection:
            value += f'[{", ".join(repr(c) for c in self.selection)}]'
        if self.prefilter:
            value += f'.where({repr(self.prefilter)})'
        if self.grouping:
            value += f'.groupby({", ".join(repr(c) for c in self.grouping)})'
        if self.postfilter:
            value += f'.having({repr(self.postfilter)})'
        if self.ordering:
            value += f'.orderby({", ".join(repr(c) for c in self.ordering)})'
        if self.rows:
            value += f'[{repr(self.rows)}]'
        return value

    @property
    def query(self) -> 'Query':
        return self

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence['series.Column']:
        """Get the list of columns supplied by this query.

        Returns:
            A sequence of supplying columns.
        """
        return self.selection if self.selection else self.source.columns

    def accept(self, visitor: visit.Frame) -> None:
        visitor.visit_query(self)

    def select(self, *columns: 'series.Column') -> 'Query':
        return Query(self.source, columns, self.prefilter, self.grouping, self.postfilter, self.ordering, self.rows)

    def where(self, condition: 'series.Expression') -> 'Query':
        if self.prefilter is not None:
            condition &= self.prefilter
        return Query(self.source, self.selection, condition, self.grouping, self.postfilter, self.ordering, self.rows)

    def having(self, condition: 'series.Expression') -> 'Query':
        if self.postfilter is not None:
            condition &= self.postfilter
        return Query(self.source, self.selection, self.prefilter, self.grouping, condition, self.ordering, self.rows)

    def join(
        self,
        other: Queryable,
        condition: typing.Optional['series.Expression'] = None,
        kind: typing.Optional[typing.Union[Join.Kind, str]] = None,
    ) -> 'Query':
        return Query(
            Join(self.source, other, condition, kind),
            self.selection,
            self.prefilter,
            self.grouping,
            self.postfilter,
            self.ordering,
            self.rows,
        )

    def groupby(self, *columns: 'series.Operable') -> 'Query':
        return Query(self.source, self.selection, self.prefilter, columns, self.postfilter, self.ordering, self.rows)

    def orderby(
        self,
        *columns: typing.Union[
            'series.Operable',
            typing.Union['series.Ordering.Direction', str],
            typing.Tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
        ],
    ) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, columns, self.rows)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        return Query(
            self.source,
            self.selection,
            self.prefilter,
            self.grouping,
            self.postfilter,
            self.ordering,
            Rows(count, offset),
        )
