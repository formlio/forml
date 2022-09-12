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
import copyreg
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

if typing.TYPE_CHECKING:
    from forml.io import dsl


LOGGER = logging.getLogger(__name__)


class Rows(typing.NamedTuple):
    """Row limit spec container.

    Attention:
        Instances are expected to be created internally via :meth:`dsl.Queryable.limit
        <forml.io.dsl.Queryable.limit>`.
    """

    count: int
    """Number of rows to return."""
    offset: int = 0
    """Skip the given number of rows."""

    def __repr__(self):
        return f'{self.offset}:{self.count}'


class Source(tuple, metaclass=abc.ABCMeta):
    """Base class of the *tabular* data frame sources.

    A *Source* is anything that can be used to obtain tabular data *FROM*. It is a logical
    collection of :class:`dsl.Feature <forml.io.dsl.Feature>` instances represented by its
    :attr:`schema`.
    """

    class Schema(type):
        """Meta-class for schema types construction.

        It guarantees consistent hashing and comparability for equality of the produced schema
        classes.

        Attention:
            This meta-class is used internally, for schema frontend API see the :class:`dsl.Schema
            <forml.io.dsl.Schema>`.
        """

        def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
            seen = set()
            existing = collections.ChainMap(
                *(
                    {f.name: k}
                    for b in bases
                    if isinstance(b, Source.Schema)
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
            cls = super().__new__(mcs, name, bases, namespace)
            cls.__qualname__ = f'{name}.schema'
            return cls

        def __hash__(cls):
            # pylint: disable=not-an-iterable
            return functools.reduce(operator.xor, (hash(f) for f in cls), 0)

        def __eq__(cls, other: 'dsl.Source.Schema'):
            return (
                isinstance(other, cls.__class__) and len(cls) == len(other) and all(c == o for c, o in zip(cls, other))
            )

        def __len__(cls):
            return sum(1 for _ in cls)  # pylint: disable=not-an-iterable

        def __repr__(cls):
            return f'{cls.__module__}:{cls.__qualname__}'

        @functools.lru_cache
        def __getitem__(cls, name: str) -> 'dsl.Field':
            try:
                item = getattr(cls, name)
            except AttributeError:
                for field in cls:  # pylint: disable=not-an-iterable
                    if name == field.name:
                        return field
            else:
                if isinstance(item, _struct.Field):
                    return item
            raise KeyError(f'Unknown field {name}')

        def __iter__(cls) -> typing.Iterator['dsl.Field']:
            return iter(
                {
                    k: f
                    for c in reversed(inspect.getmro(cls))
                    for k, f in c.__dict__.items()
                    if isinstance(f, _struct.Field)
                }.values()
            )

    copyreg.pickle(
        Schema,
        lambda s: (
            Source.Schema,
            (s.__name__, s.__bases__, {k: f for k, f in s.__dict__.items() if isinstance(f, _struct.Field)}),
        ),
    )

    class Visitor:
        """Source visitor."""

        def visit_source(self, source: 'dsl.Source') -> None:  # pylint: disable=unused-argument
            """Generic source hook.

            Args:
                source: Source instance to be visited.
            """

        def visit_table(self, source: 'dsl.Table') -> None:
            """Table hook.

            Args:
                source: Source instance to be visited.
            """
            self.visit_source(source)

        def visit_reference(self, source: 'dsl.Reference') -> None:
            """Reference hook.

            Args:
                source: Instance to be visited.
            """
            source.instance.accept(self)
            self.visit_source(source)

        def visit_join(self, source: 'dsl.Join') -> None:
            """Join hook.

            Args:
                source: Instance to be visited.
            """
            source.left.accept(self)
            source.right.accept(self)
            self.visit_source(source)

        def visit_set(self, source: 'dsl.Set') -> None:
            """Set hook.

            Args:
                source: Instance to be visited.
            """
            source.left.accept(self)
            source.right.accept(self)
            self.visit_source(source)

        def visit_query(self, source: 'dsl.Query') -> None:
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
        return hash(self.__class__.__module__) ^ hash(self.__class__.__qualname__) ^ super().__hash__()

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(a) for a in self)})'

    def __getattr__(self, name: str) -> 'dsl.Feature':
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
    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """

    @functools.cached_property
    def schema(self) -> 'dsl.Source.Schema':
        """Schema type representing this source.

        Returns:
            Schema type.
        """
        return self.Schema(
            self.__class__.__name__,
            (_struct.Schema.schema,),
            {(c.name or f'_{i}'): _struct.Field(c.kind, c.name) for i, c in enumerate(self.features)},
        )

    @functools.cached_property
    @abc.abstractmethod
    def features(self) -> typing.Sequence['dsl.Feature']:
        """List of features logically contained in or potentially produced by this Source.

        Returns:
            Sequence of contained features.
        """

    @property
    def query(self) -> 'dsl.Query':
        """Query equivalent of this Source.

        Returns:
            Query instance.
        """
        return Query(self)

    @property
    def statement(self) -> 'dsl.Statement':
        """Statement equivalent of this Source.

        Returns:
            Statement instance.
        """
        return self.query

    @property
    def instance(self) -> 'dsl.Source':
        """Return the source instance.

        Apart from the ``Reference`` type is the Source itself.

        Returns:
            Source instance.
        """
        return self

    def reference(self, name: typing.Optional[str] = None) -> 'dsl.Reference':
        """Get an independent reference to this Source (e.g. for self-join conditions).

        Args:
            name: Optional alias to be used for this reference (random by default).

        Returns:
            New reference to this Source.

        Examples:
            >>> manager = staff.Employee.reference('manager')
            >>> subs = (
            ...     manager.join(staff.Employee, staff.Employee.manager == manager.id)
            ...     .select(manager.name, function.Count(staff.Employee.id).alias('subs'))
            ...     .groupby(manager.id)
            ... )
        """
        return Reference(self, name)

    def union(self, other: 'dsl.Source') -> 'dsl.Set':
        """Create a new Source as a set union of this and the other Source.

        Args:
            other: Source to union with.

        Returns:
            Set instance.

        Examples:
            >>> barbaz = (
            ...     foo.Bar.select(foo.Bar.X, foo.Bar.Y)
            ...     .union(foo.Baz.select(foo.Baz.X, foo.Baz.Y))
            ... )
        """
        return Set(self, other, Set.Kind.UNION)

    def intersection(self, other: 'dsl.Source') -> 'dsl.Set':
        """Create a new Source as a set intersection of this and the other Source.

        Args:
            other: Source to intersect with.

        Returns:
            Set instance.

        Examples:
            >>> barbaz = (
            ...     foo.Bar.select(foo.Bar.X, foo.Bar.Y)
            ...     .intersection(foo.Baz.select(foo.Baz.X, foo.Baz.Y))
            ... )
        """
        return Set(self, other, Set.Kind.INTERSECTION)

    def difference(self, other: 'dsl.Source') -> 'dsl.Set':
        """Create a new Source as a set difference of this and the other Source.

        Args:
            other: Source to difference with.

        Returns:
            Set instance.

        Examples:
            >>> barbaz = (
            ...     foo.Bar.select(foo.Bar.X, foo.Bar.Y)
            ...     .difference(foo.Baz.select(foo.Baz.X, foo.Baz.Y))
            ... )
        """
        return Set(self, other, Set.Kind.DIFFERENCE)


class Statement(Source, metaclass=abc.ABCMeta):
    """Base class for complete statements.

    Complete statements are:

    * :class:`forml.io.dsl.Query`
    * :class:`forml.io.dsl.Set`.
    """


class Set(Statement):
    """Source made of two set-combined sub-statements with the same schema.

    Attention:
        Instances are expected to be created internally via:

        * :meth:`dsl.Source.union() <forml.io.dsl.Source.union>`
        * :meth:`dsl.Source.intersection() <forml.io.dsl.Source.intersection>`
        * :meth:`dsl.Source.difference() <forml.io.dsl.Source.difference>`
    """

    @enum.unique
    class Kind(enum.Enum):
        """Set type enum."""

        UNION = 'union'
        """Union set operation type."""
        INTERSECTION = 'intersection'
        """Intersection set operation type."""
        DIFFERENCE = 'difference'
        """Difference set operation type."""

    left: 'dsl.Statement' = property(operator.itemgetter(0))
    """Left side of the set operation."""
    right: 'dsl.Statement' = property(operator.itemgetter(1))
    """Right side of the set operation."""
    kind: 'dsl.Set.Kind' = property(operator.itemgetter(2))
    """Set operation enum type."""

    def __new__(cls, left: 'dsl.Source', right: 'dsl.Source', kind: 'dsl.Set.Kind'):
        if left.schema != right.schema:
            raise _exception.GrammarError('Incompatible sources')
        return super().__new__(cls, left.statement, right.statement, kind)

    def __repr__(self):
        return f'{repr(self.left)} {self.kind.value} {repr(self.right)}'

    @property
    def statement(self) -> 'dsl.Statement':
        return self

    @functools.cached_property
    def features(self) -> typing.Sequence['dsl.Feature']:
        return self.left.features + self.right.features

    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        visitor.visit_set(self)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for any *Source* that can be queried directly."""

    def select(self, *features: 'dsl.Feature') -> 'dsl.Query':
        """Specify the output features to be provided (projection).

        Repeated calls to ``.select`` replace the earlier selection.

        Args:
            features: Sequence of features.

        Returns:
            Query instance.

        Examples:
            >>> barxy = foo.Bar.select(foo.Bar.X, foo.Bar.Y)
        """
        return self.query.select(*features)

    def where(self, condition: 'dsl.Predicate') -> 'dsl.Query':
        """Add a row-filtering condition that's evaluated before any aggregations.

        Repeated calls to ``.where`` combine all the conditions (logical AND).

        Args:
            condition: Boolean feature expression.

        Returns:
            Query instance.

        Examples:
            >>> barx10 = foo.Bar.where(foo.Bar.X == 10)
        """
        return self.query.where(condition)

    def having(self, condition: 'dsl.Predicate') -> 'dsl.Query':
        """Add a row-filtering condition that's applied to the evaluated aggregations.

        Repeated calls to ``.having`` combine all the conditions (logical AND).

        Args:
            condition: Boolean feature expression.

        Returns:
            Query instance.

        Examples:
            >>> bargy10 = foo.Bar.groupby(foo.Bar.X).having(function.Count(foo.Bar.Y) == 10)
        """
        return self.query.having(condition)

    def groupby(self, *features: 'dsl.Operable') -> 'dsl.Query':
        """Aggregation grouping specifiers.

        Repeated calls to ``.groupby`` replace the earlier grouping.

        Args:
            features: Sequence of aggregation features.

        Returns:
            Query instance.

        Examples:
            >>> bargbx = foo.Bar.groupby(foo.Bar.X).select(foo.Bar.X, function.Count(foo.Bar.Y))
        """
        return self.query.groupby(*features)

    def orderby(self, *terms: 'dsl.Ordering.Term') -> 'dsl.Query':
        """Ordering specifiers.

        Default direction is *ascending*.

        Repeated calls to ``.orderby`` replace the earlier ordering.

        Args:
            terms: Sequence of feature and direction tuples.

        Returns:
            Query instance.

        Examples:
            >>> barbyx = foo.Bar.orderby(foo.Bar.X)
            >>> barbyxd = foo.Bar.orderby(foo.Bar.X, 'desc')
            >>> barbxy = foo.Bar.orderby(foo.Bar.X, foo.Bar.Y)
            >>> barbxdy = foo.Bar.orderby(
            ...     foo.Bar.X, dsl.Ordering.Direction.DESCENDING, foo.Bar.Y, 'asc'
            ... )
            >>> barbydxd = foo.Bar.orderby(
            ...     (foo.Bar.X, 'desc'),
            ...     (foo.Bar.Y, dsl.Ordering.Direction.DESCENDING),
            ... )
        """
        return self.query.orderby(*terms)

    def limit(self, count: int, offset: int = 0) -> 'dsl.Query':
        """Restrict the result rows by its max *count* with an optional *offset*.

        Repeated calls to ``.limit`` replace the earlier restriction.

        Args:
            count: Number of rows to return.
            offset: Skip the given number of rows.

        Returns:
            Query instance.

        Examples:
            >>> bar10 = foo.Bar.limit(10)
        """
        return self.query.limit(count, offset)


class Origin(Queryable, metaclass=abc.ABCMeta):
    """Origin is a queryable Source with some handle.

    Its features are represented using :class:`dsl.Element <forml.io.dsl.Element>`.
    """

    @property
    @abc.abstractmethod
    def features(self) -> typing.Sequence['dsl.Element']:
        """Origin features are instances of ``dsl.Element``.

        Returns:
            Sequence of ``dsl.Element`` instances.
        """

    def inner_join(self, other: 'dsl.Origin', condition: 'dsl.Predicate') -> 'dsl.Join':
        """Construct an *inner* join with the other *origin* using the provided *condition*.

        Args:
            other: Source to join with.
            condition: Feature expression as the join condition.

        Returns:
            Join instance.

        Examples:
            >>> barbaz = foo.Bar.inner_join(foo.Baz, foo.Bar.baz == foo.Baz.id)
        """
        return Join(self, other, Join.Kind.INNER, condition)

    def left_join(self, other: 'dsl.Origin', condition: 'dsl.Predicate') -> 'dsl.Join':
        """Construct a *left* join with the other *origin* using the provided *condition*.

        Args:
            other: Source to join with.
            condition: Feature expression as the join condition.

        Returns:
            Join instance.

        Examples:
            >>> barbaz = foo.Bar.left_join(foo.Baz, foo.Bar.baz == foo.Baz.id)
        """
        return Join(self, other, Join.Kind.LEFT, condition)

    def right_join(self, other: 'dsl.Origin', condition: 'dsl.Predicate') -> 'dsl.Join':
        """Construct a *right* join with the other *origin* using the provided *condition*.

        Args:
            other: Source to join with.
            condition: Feature expression as the join condition.

        Returns:
            Join instance.

        Examples:
            >>> barbaz = foo.Bar.right_join(foo.Baz, foo.Bar.baz == foo.Baz.id)
        """
        return Join(self, other, Join.Kind.RIGHT, condition)

    def full_join(self, other: 'dsl.Origin', condition: 'dsl.Predicate') -> 'dsl.Join':
        """Construct a *full* join with the other *origin* using the provided *condition*.

        Args:
            other: Source to join with.
            condition: Feature expression as the join condition.

        Returns:
            Join instance.

        Examples:
            >>> barbaz = foo.Bar.full_join(foo.Baz, foo.Bar.baz == foo.Baz.id)
        """
        return Join(self, other, Join.Kind.FULL, condition)

    def cross_join(self, other: 'dsl.Origin') -> 'dsl.Join':
        """Construct a *cross* join with the other *origin*.

        Args:
            other: Source to join with.

        Returns:
            Join instance.

        Examples:
            >>> barbaz = foo.Bar.cross_join(foo.Baz)
        """
        return Join(self, other, kind=Join.Kind.CROSS)


class Join(Origin):
    """Source made of two join-combined sub-sources.

    Attention:
        Instances are expected to be created internally via:

        * :meth:`dsl.Origin.inner_join() <forml.io.dsl.Origin.inner_join>`
        * :meth:`dsl.Origin.left_join() <forml.io.dsl.Origin.left_join>`
        * :meth:`dsl.Origin.right_join() <forml.io.dsl.Origin.right_join>`
        * :meth:`dsl.Origin.full_join() <forml.io.dsl.Origin.full_join>`
        * :meth:`dsl.Origin.cross_join() <forml.io.dsl.Origin.cross_join>`
    """

    @enum.unique
    class Kind(enum.Enum):
        """Join type enum."""

        INNER = 'inner'
        """Inner join type (default if *condition* is provided)."""
        LEFT = 'left'
        """Left outer join type."""
        RIGHT = 'right'
        """Right outer join type."""
        FULL = 'full'
        """Full join type."""
        CROSS = 'cross'
        """Cross join type (default if *condition* is not provided)."""

        def __repr__(self):
            return f'<{self.value}-join>'

    left: 'dsl.Origin' = property(operator.itemgetter(0))
    """Left side of the join operation."""
    right: 'dsl.Origin' = property(operator.itemgetter(1))
    """Right side of the join operation."""
    kind: 'dsl.Join.Kind' = property(operator.itemgetter(2))
    """Join type."""
    condition: typing.Optional['dsl.Predicate'] = property(operator.itemgetter(3))
    """Join condition (invalid for *CROSS*-join)."""

    def __new__(
        cls,
        left: 'dsl.Origin',
        right: 'dsl.Origin',
        kind: typing.Union['dsl.Join.Kind', str],
        condition: typing.Optional['dsl.Predicate'] = None,
    ):
        if (kind is cls.Kind.CROSS) ^ (condition is None):
            raise _exception.GrammarError('Illegal use of condition and join type')
        if condition is not None:
            condition = series.Cumulative.ensure_notin(series.Predicate.ensure_is(condition))
            if not series.Element.dissect(condition).issubset(series.Element.dissect(*left.features, *right.features)):
                raise _exception.GrammarError(
                    f'({condition}) not a subset of source features ({left.features}, {right.features})'
                )
        return super().__new__(cls, left, right, kind, condition)

    def __repr__(self):
        return f'{repr(self.left)}{repr(self.kind)}{repr(self.right)}'

    @functools.cached_property
    def features(self) -> typing.Sequence['dsl.Element']:
        return self.left.features + self.right.features

    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        visitor.visit_join(self)


class Reference(Origin):
    """Wrapper around any *Source* associating it with a (possibly random) name.

    Attention:
        Instances are expected to be created internally via :meth:`dsl.Source.reference
        <forml.io.dsl.Source.reference>`.
    """

    _NAMELEN: int = 8
    instance: 'dsl.Source' = property(operator.itemgetter(0))
    """Wrapped *Source* instance."""
    name: str = property(operator.itemgetter(1))
    """Reference name."""

    def __new__(cls, instance: 'dsl.Source', name: typing.Optional[str] = None):
        if not name:
            name = ''.join(random.choice(string.ascii_lowercase) for _ in range(cls._NAMELEN))
        return super().__new__(cls, instance.instance, name)

    def __repr__(self):
        return f'{self.name}=[{repr(self.instance)}]'

    @functools.cached_property
    def features(self) -> typing.Sequence['dsl.Element']:
        return tuple(series.Element(self, c.name) for c in self.instance.features)

    @property
    def schema(self) -> 'dsl.Source.Schema':
        return self.instance.schema

    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_reference(self)


class Table(Origin):
    """Table based *Source* with an explicit *schema*.

    Attention:
        The primary way of creating ``Table`` instances is by inheriting the :class:`dsl.Schema
        <forml.io.dsl.Schema>` which is using this type as a meta-class.
    """

    class Meta(abc.ABCMeta):
        """Metaclass for dynamic parent classes."""

    copyreg.pickle(
        Meta,
        lambda c: (
            Table.Meta,
            (c.__name__, c.__bases__, {}),
        ),
    )

    @typing.overload
    def __new__(  # pylint: disable=bad-classmethod-argument
        mcs,
        name: str,
        bases: tuple[type],
        namespace: dict[str, typing.Any],
    ):
        """Meta-class mode constructor.

        Args:
            name: Table class name.
            bases: Table base classes.
            namespace: Class namespace container.
        """

    @typing.overload
    def __new__(cls, schema: 'dsl.Source.Schema'):
        """Standard class mode constructor.

        Args:
            schema: Table *schema* type.
        """

    def __new__(mcs, schema, bases=None, namespace=None):  # pylint: disable=bad-classmethod-argument
        if isinstance(schema, str):  # used as metaclass
            if bases:
                bases = tuple(b.schema for b in bases if isinstance(b, Table))
                # strip the parent base class and namespace
                mcs = mcs.Meta(schema, mcs.__bases__, {})  # pylint: disable=self-cls-assignment
            elif not any(isinstance(a, _struct.Field) for a in namespace.values()):
                # used as a base class definition - let's propagate the namespace
                mcs = mcs.Meta(schema, (mcs,), namespace)  # pylint: disable=self-cls-assignment
            schema = mcs.Schema(schema, bases, namespace)
        elif bases or namespace:
            raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, schema)  # used as constructor

    def __repr__(self):
        return self.schema.__name__

    @property
    def schema(self) -> 'dsl.Source.Schema':
        return self[0]

    @functools.cached_property
    def features(self) -> typing.Sequence['dsl.Column']:
        return tuple(series.Column(self, f.name) for f in self.schema)

    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        visitor.visit_table(self)


class Query(Queryable, Statement):
    """Query based *Source*.

    Container for holding all the parameters supplied via the :class:`dsl.Queryable
    <forml.io.dsl.Queryable>` interface.

    Attention:
        Instances are expected to be created internally via the ``dsl.Queryable`` interface methods.
    """

    source: 'dsl.Source' = property(operator.itemgetter(0))
    """Base *Source* to query *FROM*."""
    selection: tuple['dsl.Feature'] = property(operator.itemgetter(1))
    """Result projection features."""
    prefilter: typing.Optional['dsl.Predicate'] = property(operator.itemgetter(2))
    """Row-filtering condition to be applied before potential aggregations."""
    grouping: tuple['dsl.Operable'] = property(operator.itemgetter(3))
    """Aggregation grouping specifiers."""
    postfilter: typing.Optional['dsl.Predicate'] = property(operator.itemgetter(4))
    """Row-filtering condition to be applied after aggregations."""
    ordering: tuple['dsl.Ordering'] = property(operator.itemgetter(5))
    """Ordering specifiers."""
    rows: typing.Optional['dsl.Rows'] = property(operator.itemgetter(6))
    """Row restriction limit."""

    def __new__(
        cls,
        source: 'dsl.Source',
        selection: typing.Optional[typing.Iterable['dsl.Feature']] = None,
        prefilter: typing.Optional['dsl.Predicate'] = None,
        grouping: typing.Optional[typing.Iterable['dsl.Operable']] = None,
        postfilter: typing.Optional['dsl.Predicate'] = None,
        ordering: typing.Optional[typing.Sequence['dsl.Ordering.Term']] = None,
        rows: typing.Optional['dsl.Rows'] = None,
    ):
        def ensure_subset(*features: 'dsl.Feature') -> typing.Sequence['dsl.Feature']:
            """Ensure the provided features is a valid subset of the available Source features.

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
        ordering = tuple(series.Ordering.make(*(ordering or [])))
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
    def query(self) -> 'dsl.Query':
        return self

    @functools.cached_property
    def features(self) -> typing.Sequence['dsl.Feature']:
        """Get the list of features supplied by this query.

        Returns:
            A sequence of supplying features.
        """
        return self.selection if self.selection else self.source.features

    def accept(self, visitor: 'dsl.Source.Visitor') -> None:
        visitor.visit_query(self)

    def select(self, *features: 'dsl.Feature') -> 'dsl.Query':
        return Query(self.source, features, self.prefilter, self.grouping, self.postfilter, self.ordering, self.rows)

    def where(self, condition: 'dsl.Predicate') -> 'dsl.Query':
        if self.prefilter is not None:
            condition &= self.prefilter
        return Query(self.source, self.selection, condition, self.grouping, self.postfilter, self.ordering, self.rows)

    def having(self, condition: 'dsl.Predicate') -> 'dsl.Query':
        if self.postfilter is not None:
            condition &= self.postfilter
        return Query(self.source, self.selection, self.prefilter, self.grouping, condition, self.ordering, self.rows)

    def groupby(self, *features: 'dsl.Operable') -> 'dsl.Query':
        return Query(self.source, self.selection, self.prefilter, features, self.postfilter, self.ordering, self.rows)

    def orderby(self, *terms: 'dsl.Ordering.Term') -> 'dsl.Query':
        return Query(self.source, self.selection, self.prefilter, self.grouping, self.postfilter, terms, self.rows)

    def limit(self, count: int, offset: int = 0) -> 'dsl.Query':
        return Query(
            self.source,
            self.selection,
            self.prefilter,
            self.grouping,
            self.postfilter,
            self.ordering,
            Rows(count, offset),
        )
