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

from forml.io import etl
from forml.io.dsl import error
from forml.io.dsl.schema import series, visit

LOGGER = logging.getLogger(__name__)


class Rows(typing.NamedTuple):
    """Row limit spec.
    """
    count: int
    offset: int = 0

    def __repr__(self):
        return f'{self.offset}:{self.count}'


class Source(tuple, metaclass=abc.ABCMeta):
    """Source base class.
    """
    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(a) for a in self)})'

    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence[series.Column]:
        """Get the list of columns supplied by this source.

        Returns: Sequence of supplied columns.
        """

    @property
    @abc.abstractmethod
    def explicit(self) -> typing.AbstractSet[series.Field]:
        """Set of schema fields explicitly referred within this source. In a way contrary to the .columns, this is
        a demand side of the given source.

        Returns: Set of explicit schema fields.
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
    def accept(self, visitor: visit.Frame) -> None:
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
        INNER = 'inner'
        LEFT = 'left'
        RIGHT = 'right'
        FULL = 'full'
        CROSS = 'cross'

        def __repr__(self):
            return f'<{self.value}-join>'

    left: 'Tangible' = property(operator.itemgetter(0))
    right: 'Tangible' = property(operator.itemgetter(1))
    condition: typing.Optional[series.Expression] = property(operator.itemgetter(2))
    kind: 'Join.Kind' = property(operator.itemgetter(3))

    def __new__(cls, left: 'Tangible', right: 'Tangible', condition: typing.Optional[series.Expression] = None,
                kind: typing.Optional[typing.Union['Join.Kind', str]] = None):
        kind = cls.Kind(kind) if kind else cls.Kind.INNER if condition is not None else cls.Kind.CROSS
        if condition is not None:
            if kind is cls.Kind.CROSS:
                raise error.Syntax('Illegal use of condition for cross-join')
            if not series.Field.dissect(condition).issubset(series.Field.dissect(*left.columns, *right.columns)):
                raise error.Syntax(f'({condition}) not a subset of source columns ({left.columns}, {right.columns})')
            condition = series.Multirow.ensure_notin(series.Logical.ensure_is(series.Operable.ensure_is(condition)))
        return super().__new__(cls, [left, right, condition, kind])

    def __repr__(self):
        return f'{repr(self.left)}{repr(self.kind)}{repr(self.right)}'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    @property
    @functools.lru_cache()
    def explicit(self) -> typing.AbstractSet[series.Field]:
        fields = self.left.explicit.union(self.right.explicit)
        if self.condition is not None:
            fields |= {f for f in series.Field.dissect(self.condition) if isinstance(f.source, Table)}
        return frozenset(fields)

    def accept(self, visitor: visit.Frame) -> None:
        with visitor.visit_join(self):
            self.left.accept(visitor)
            self.right.accept(visitor)


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

    def __repr__(self):
        return f'{repr(self.left)} {self.kind.value} {repr(self.right)}'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    @property
    @functools.lru_cache()
    def explicit(self) -> typing.AbstractSet[series.Field]:
        return frozenset(self.left.explicit.union(self.right.explicit))

    def accept(self, visitor: visit.Frame) -> None:
        with visitor.visit_set(self):
            self.left.accept(visitor)
            self.right.accept(visitor)


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

    @property
    def instance(self) -> 'Source':
        """Return the source instance - which apart from the Reference type is the source itself.

        Returns: Source instance.
        """
        return self

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

    def join(self, other: 'Tangible', condition: typing.Optional[series.Expression] = None,
             kind: typing.Optional[typing.Union[Join.Kind, str]] = None) -> 'Query':
        """Join with other tangible.
        """
        return self.query.join(other, condition, kind)

    def groupby(self, *columns: series.Operable) -> 'Query':
        """Aggregating spec.
        """
        return self.query.groupby(*columns)

    def orderby(self, *columns: typing.Union[series.Operable, typing.Union[
            series.Ordering.Direction, str], typing.Tuple[
            series.Operable, typing.Union[series.Ordering.Direction, str]]]) -> 'Query':
        """series.Ordering spec.
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

    def __repr__(self):
        return f'{self.name}=[{repr(self.instance)}]'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Field]:
        return tuple(series.Field(self, c.name) for c in self.instance.columns)

    @property
    def explicit(self) -> typing.AbstractSet[series.Field]:
        return self.instance.explicit

    @property
    def schema(self) -> typing.Type['etl.Schema']:
        return self.instance.schema

    def reference(self, name: typing.Optional[str] = None) -> 'Reference':
        return Reference(self.instance, name)

    def accept(self, visitor: visit.Frame) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        with visitor.visit_reference(self):
            self.instance.accept(visitor)


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
                    raise error.Syntax(f'Colliding field name {new} in schema {name}')
                existing.add(new)
            if bases and not existing:
                raise error.Syntax(f'No fields defined for schema {name}')
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

    def __repr__(self):
        return f'{self.schema.__name__}'

    @property
    @functools.lru_cache()
    def columns(self) -> typing.Sequence[series.Field]:
        return tuple(series.Field(self, f.name or k) for k, f in self.schema.items())

    @property
    @functools.lru_cache()
    def explicit(self) -> typing.AbstractSet[series.Field]:
        return frozenset()  # no fields are explicit on a bare table

    def accept(self, visitor: visit.Frame) -> None:
        with visitor.visit_table(self):
            pass


class Query(Queryable):
    """Generic source descriptor.
    """
    source: Source = property(operator.itemgetter(0))
    selection: typing.Tuple[series.Column] = property(operator.itemgetter(1))
    prefilter: typing.Optional[series.Expression] = property(operator.itemgetter(2))
    grouping: typing.Tuple[series.Operable] = property(operator.itemgetter(3))
    postfilter: typing.Optional[series.Expression] = property(operator.itemgetter(4))
    ordering: typing.Tuple[series.Ordering] = property(operator.itemgetter(5))
    rows: typing.Optional[Rows] = property(operator.itemgetter(6))

    def __new__(cls, source: Source,
                selection: typing.Optional[typing.Iterable[series.Column]] = None,
                prefilter: typing.Optional[series.Expression] = None,
                grouping: typing.Optional[typing.Iterable[series.Operable]] = None,
                postfilter: typing.Optional[series.Expression] = None,
                ordering: typing.Optional[typing.Sequence[typing.Union[series.Operable,
                                                                       typing.Union[series.Ordering.Direction, str],
                                                                       typing.Tuple[series.Operable, typing.Union[
                                                                           series.Ordering.Direction, str]]]]] = None,
                rows: typing.Optional[Rows] = None):

        def ensure_subset(*columns: series.Column) -> typing.Sequence[series.Column]:
            """Ensure the provided columns is a valid subset of the available source columns.

            Args:
                *columns: List of columns to validate.

            Returns: Original list of columns if all valid.
            """
            if not series.Field.dissect(*columns).issubset(superset):
                raise error.Syntax(f'{columns} not a subset of source columns: {superset}')
            return columns

        superset = series.Field.dissect(*source.columns)
        if prefilter is not None:
            prefilter = series.Multirow.ensure_notin(series.Logical.ensure_is(
                series.Operable.ensure_is(*ensure_subset(prefilter))))
        if grouping:
            grouping = [series.Multirow.ensure_notin(series.Operable.ensure_is(g)) for g in ensure_subset(*grouping)]
            for aggregate in {c.operable for c in selection or source.columns}.difference(grouping):
                series.Aggregate.ensure_in(aggregate)
        if postfilter is not None:
            postfilter = series.Window.ensure_notin(series.Logical.ensure_is(
                series.Operable.ensure_is(*ensure_subset(postfilter))))
        ordering = tuple(series.Ordering.make(ordering or []))
        ensure_subset(*(o.column for o in ordering))
        return super().__new__(cls, [source, tuple(ensure_subset(*(selection or []))), prefilter, tuple(grouping or []),
                                     postfilter, ordering, rows])

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
    def columns(self) -> typing.Sequence[series.Column]:
        return self.selection if self.selection else self.source.columns

    @property
    @functools.lru_cache()
    def explicit(self) -> typing.AbstractSet[series.Field]:
        columns = set(self.columns).union(self.grouping).union(o.column for o in self.ordering)
        if self.prefilter is not None:
            columns.add(self.prefilter)
        if self.postfilter is not None:
            columns.add(self.postfilter)
        return frozenset(self.source.explicit.union({
            f for f in series.Field.dissect(*columns) if isinstance(f.source, Table)}))

    def accept(self, visitor: visit.Frame) -> None:
        with visitor.visit_query(self):
            self.source.accept(visitor)

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

    def join(self, other: Queryable, condition: typing.Optional[series.Expression] = None,
             kind: typing.Optional[typing.Union[Join.Kind, str]] = None) -> 'Query':
        return Query(Join(self.source, other, condition, kind), self.selection, self.prefilter, self.grouping,
                     self.postfilter, self.ordering, self.rows)

    def groupby(self, *columns: series.Operable) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, columns, self.postfilter, self.ordering, self.rows)

    def orderby(self, *columns: typing.Union[series.Operable, typing.Union[
            series.Ordering.Direction, str], typing.Tuple[series.Operable, typing.Union[
            series.Ordering.Direction, str]]]) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, columns, self.rows)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, self.ordering,
                     Rows(count, offset))
