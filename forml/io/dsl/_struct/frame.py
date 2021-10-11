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
import logging
import operator
import random
import string
import typing

from .. import _exception, _struct
from . import series

LOGGER = logging.getLogger(__name__)


class Rows(typing.NamedTuple):
    """Row limit spec."""

    count: int
    offset: int = 0

    def __repr__(self):
        return f'{self.offset}:{self.count}'


class Source(tuple, metaclass=abc.ABCMeta):
    """Source base class."""

    class Visitor:
        """Source visitor."""

        def visit_source(self, source: 'Source') -> None:  # pylint: disable=unused-argument, no-self-use
            """Generic source hook.

            Args:
                source: Source instance to be visited.
            """

        def visit_table(self, source: 'Table') -> None:
            """Table hook.

            Args:
                source: Source instance to be visited.
            """
            self.visit_source(source)

        def visit_reference(self, source: 'Reference') -> None:
            """Reference hook.

            Args:
                source: Instance to be visited.
            """
            source.instance.accept(self)
            self.visit_source(source)

        def visit_join(self, source: 'Join') -> None:
            """Join hook.

            Args:
                source: Instance to be visited.
            """
            source.left.accept(self)
            source.right.accept(self)
            self.visit_source(source)

        def visit_set(self, source: 'Set') -> None:
            """Set hook.

            Args:
                source: Instance to be visited.
            """
            source.left.accept(self)
            source.right.accept(self)
            self.visit_source(source)

        def visit_query(self, source: 'Query') -> None:
            """Query hook.

            Args:
                source: Instance to be visited.
            """
            source.source.accept(self)
            self.visit_source(source)

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
    def features(self) -> typing.Sequence['series.Feature']:
        """Get the list of features supplied by this source.

        Returns:
            Sequence of supplying features.
        """

    @property
    @functools.lru_cache
    def schema(self) -> type['_struct.Schema']:
        """Get the schema type for this source.

        Returns:
            Schema type.
        """
        return Table.Schema(
            f'{self.__class__.__name__}Schema',
            (_struct.Schema.schema,),
            {(c.name or f'_{i}'): _struct.Field(c.kind, c.name) for i, c in enumerate(self.features)},
        )

    def __getattr__(self, name: str) -> 'series.Feature':
        try:
            return self[name]
        except KeyError as err:
            raise AttributeError(f'Invalid feature {name}') from err

    @functools.lru_cache
    def __getitem__(self, name: typing.Union[int, str]) -> typing.Any:
        try:
            return super().__getitem__(name)
        except (TypeError, IndexError) as err:
            name = self.schema[name].name
            for field, feature in zip(self.schema, self.features):
                if name == field.name:
                    return feature
            raise RuntimeError(f'Inconsistent {name} lookup vs schema iteration') from err

    @abc.abstractmethod
    def accept(self, visitor: Visitor) -> None:
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
                raise _exception.GrammarError('Illegal use of condition for cross-join')
            condition = series.Cumulative.ensure_notin(series.Predicate.ensure_is(condition))
            if not series.Element.dissect(condition).issubset(series.Element.dissect(*left.features, *right.features)):
                raise _exception.GrammarError(
                    f'({condition}) not a subset of source features ({left.features}, {right.features})'
                )
        return super().__new__(cls, left, right, condition, kind)

    def __repr__(self):
        return f'{repr(self.left)}{repr(self.kind)}{repr(self.right)}'

    @property
    @functools.lru_cache
    def features(self) -> typing.Sequence['series.Feature']:
        return self.left.features + self.right.features

    def accept(self, visitor: Source.Visitor) -> None:
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
    @functools.lru_cache
    def features(self) -> typing.Sequence['series.Feature']:
        return self.left.features + self.right.features

    def accept(self, visitor: Source.Visitor) -> None:
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

    def select(self, *features: 'series.Feature') -> 'Query':
        """Specify the output features to be provided (projection).

        Args:
            features: Sequence of feature expressions.

        Returns:
            Query instance.
        """
        return self.query.select(*features)

    def where(self, condition: 'series.Expression') -> 'Query':
        """Add a row-filtering condition that's evaluated before any aggregations.

        Repeated calls to ``.where`` combine all the conditions (logical AND).

        Args:
            condition: Boolean feature expression.

        Returns:
            Query instance.
        """
        return self.query.where(condition)

    def having(self, condition: 'series.Expression') -> 'Query':
        """Add a row-filtering condition that's applied to the evaluated aggregations.

        Repeated calls to ``.having`` combine all the conditions (logical AND).

        Args:
            condition: Boolean feature expression.

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
            condition: Feature expression as the join condition.
            kind: Type of the join operation (INNER, LEFT, RIGHT, FULL CROSS).

        Returns:
            Query instance.
        """
        return self.query.join(other, condition, kind)

    def groupby(self, *features: 'series.Operable') -> 'Query':
        """Aggregation specifiers.

        Args:
            features: Sequence of feature expressions.

        Returns:
            Query instance.
        """
        return self.query.groupby(*features)

    def orderby(
        self,
        *features: typing.Union[
            'series.Operable',
            typing.Union['series.Ordering.Direction', str],
            tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
        ],
    ) -> 'Query':
        """Ordering specifiers.

        Args:
            *features: Sequence of feature expressions and direction tuples.

        Returns:
            Query instance.
        """
        return self.query.orderby(*features)

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
    """Origin is a queryable that can be referenced by some handle (rather than just a statement itself) - effectively
    a Table or a subquery with a Reference.

    Its features are represented using series.Element.
    """

    @property
    @abc.abstractmethod
    def features(self) -> typing.Sequence['series.Element']:
        """Tangible features are instances of series.Element.

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
    @functools.lru_cache
    def features(self) -> typing.Sequence['series.Element']:
        return tuple(series.Element(self, c.name) for c in self.instance.features)

    @property
    def schema(self) -> type['_struct.Schema']:
        return self.instance.schema

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        return Reference(self.instance, name)

    def accept(self, visitor: Source.Visitor) -> None:
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

        def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
            seen = set()
            existing = collections.ChainMap(
                *(
                    {f.name: k}
                    for b in bases
                    if isinstance(b, Table.Schema)
                    for c in reversed(inspect.getmro(b))
                    for k, f in c.__dict__.items()
                    if isinstance(f, _struct.Field) and k not in seen and not seen.add(k)
                )
            )
            if existing and len(existing.maps) > len(existing):
                raise _exception.GrammarError(f'Colliding base classes in schema {name}')
            for key, field in namespace.items():
                if not isinstance(field, _struct.Field):
                    continue
                if not field.name:
                    namespace[key] = field = field.renamed(key)  # to normalize so that hash/eq is consistent
                if field.name in existing and existing[field.name] != key:
                    raise _exception.GrammarError(f'Colliding field name {field.name} in schema {name}')
                existing[field.name] = key
            return super().__new__(mcs, name, bases, namespace)

        def __hash__(cls):
            # pylint: disable=not-an-iterable
            return functools.reduce(operator.xor, (hash(f) for f in cls), 0)

        def __eq__(cls, other: type['_struct.Schema']):
            return len(cls) == len(other) and all(c == o for c, o in zip(cls, other))

        def __len__(cls):
            return sum(1 for _ in cls)  # pylint: disable=not-an-iterable

        def __repr__(cls):
            return f'{cls.__module__}:{cls.__qualname__}'

        @functools.lru_cache
        def __getitem__(cls, name: str) -> '_struct.Field':
            try:
                return getattr(cls, name)
            except AttributeError:
                for field in cls:  # pylint: disable=not-an-iterable
                    if name == field.name:
                        return field
            raise KeyError(f'Unknown field {name}')

        def __iter__(cls) -> typing.Iterator['_struct.Field']:
            return iter(
                {
                    k: f
                    for c in reversed(inspect.getmro(cls))
                    for k, f in c.__dict__.items()
                    if isinstance(f, _struct.Field)
                }.values()
            )

    schema: type['_struct.Schema'] = property(operator.itemgetter(0))

    def __new__(  # pylint: disable=bad-classmethod-argument
        mcs,
        schema: typing.Union[str, type['_struct.Schema']],
        bases: typing.Optional[tuple[type]] = None,
        namespace: typing.Optional[dict[str, typing.Any]] = None,
    ):
        if isinstance(schema, str):  # used as metaclass
            if bases:
                bases = (bases[0].schema,)
            schema = mcs.Schema(schema, bases, namespace)
        elif bases or namespace:
            raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, schema)  # used as constructor

    def __repr__(self):
        return self.schema.__name__

    @property
    @functools.lru_cache
    def features(self) -> typing.Sequence['series.Column']:
        return tuple(series.Column(self, f.name) for f in self.schema)

    def accept(self, visitor: Source.Visitor) -> None:
        visitor.visit_table(self)


class Query(Queryable):
    """Generic source descriptor."""

    source: Source = property(operator.itemgetter(0))
    selection: tuple['series.Feature'] = property(operator.itemgetter(1))
    prefilter: typing.Optional['series.Expression'] = property(operator.itemgetter(2))
    grouping: tuple['series.Operable'] = property(operator.itemgetter(3))
    postfilter: typing.Optional['series.Expression'] = property(operator.itemgetter(4))
    ordering: tuple['series.Ordering'] = property(operator.itemgetter(5))
    rows: typing.Optional[Rows] = property(operator.itemgetter(6))

    def __new__(
        cls,
        source: Source,
        selection: typing.Optional[typing.Iterable['series.Feature']] = None,
        prefilter: typing.Optional['series.Expression'] = None,
        grouping: typing.Optional[typing.Iterable['series.Operable']] = None,
        postfilter: typing.Optional['series.Expression'] = None,
        ordering: typing.Optional[
            typing.Sequence[
                typing.Union[
                    'series.Operable',
                    typing.Union['series.Ordering.Direction', str],
                    tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
                ]
            ]
        ] = None,
        rows: typing.Optional[Rows] = None,
    ):
        def ensure_subset(*features: 'series.Feature') -> typing.Sequence['series.Feature']:
            """Ensure the provided features is a valid subset of the available source features.

            Args:
                *features: List of features to validate.

            Returns:
                Original list of features if all valid.
            """
            if not series.Element.dissect(*features).issubset(superset):
                raise _exception.GrammarError(f'{features} not a subset of source features: {superset}')
            return features

        superset = series.Element.dissect(*source.features)
        selection = tuple(ensure_subset(*(series.Feature.ensure_is(c) for c in selection or [])))
        if prefilter is not None:
            prefilter = series.Cumulative.ensure_notin(
                series.Predicate.ensure_is(*ensure_subset(series.Operable.ensure_is(prefilter)))
            )
        if grouping:
            grouping = ensure_subset(*(series.Cumulative.ensure_notin(series.Operable.ensure_is(g)) for g in grouping))
            for aggregate in {c.operable for c in selection or source.features}.difference(grouping):
                series.Aggregate.ensure_in(aggregate)
        if postfilter is not None:
            postfilter = series.Window.ensure_notin(
                series.Predicate.ensure_is(*ensure_subset(series.Operable.ensure_is(postfilter)))
            )
        ordering = tuple(series.Ordering.make(ordering or []))
        ensure_subset(*(o.feature for o in ordering))
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
    @functools.lru_cache
    def features(self) -> typing.Sequence['series.Feature']:
        """Get the list of features supplied by this query.

        Returns:
            A sequence of supplying features.
        """
        return self.selection if self.selection else self.source.features

    def accept(self, visitor: Source.Visitor) -> None:
        visitor.visit_query(self)

    def select(self, *features: 'series.Feature') -> 'Query':
        return Query(self.source, features, self.prefilter, self.grouping, self.postfilter, self.ordering, self.rows)

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

    def groupby(self, *features: 'series.Operable') -> 'Query':
        return Query(self.source, self.selection, self.prefilter, features, self.postfilter, self.ordering, self.rows)

    def orderby(
        self,
        *features: typing.Union[
            'series.Operable',
            typing.Union['series.Ordering.Direction', str],
            tuple['series.Operable', typing.Union['series.Ordering.Direction', str]],
        ],
    ) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, features, self.rows)

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
