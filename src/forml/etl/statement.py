"""
ETL statement types.
"""
import abc
import collections
import enum
import typing

from forml.etl import schema as schemamod

if typing.TYPE_CHECKING:
    pass


class Visitor(schemamod.Visitor, metaclass=abc.ABCMeta):
    """Statement visitor.
    """
    def visit_source(self, source: 'schemamod.Source') -> None:
        """Generic source hook.

        Args:
            source: schemamod.Source instance to be visited.
        """

    def visit_join(self, source: 'Join') -> None:
        """Generic source hook.

        Args:
            source: schemamod.Source instance to be visited.
        """
        self.visit_source(source)

    def visit_set(self, source: 'Set') -> None:
        """Generic source hook.

        Args:
            source: schemamod.Source instance to be visited.
        """
        self.visit_source(source)

    def visit_query(self, source: 'Query') -> None:
        """Generic source hook.

        Args:
            source: schemamod.Source instance to be visited.
        """
        self.visit_source(source)


class Join(collections.namedtuple('Join', 'left, right, condition, kind'), schemamod.Source):
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

    def __new__(cls, left: schemamod.Source, right: schemamod.Source, condition: schemamod.Condition,
                kind: 'Join.Kind' = Kind.LEFT):
        return super().__new__(cls, left, right, condition, kind)

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        super().accept(visitor)
        visitor.visit_join(self)
        self.right.accept(visitor)


class Set(collections.namedtuple('Set', 'left, right, kind'), schemamod.Source):
    """Source made of two set-combined subsources.
    """
    @enum.unique
    class Kind(enum.Enum):
        """Set type.
        """
        UNION = 'union'
        INTERSECTION = 'intersection'
        DIFFERENCE = 'difference'

    def __new__(cls, left: schemamod.Source, right: schemamod.Source, kind: 'Set.Kind'):
        return super().__new__(cls, left, right, kind)

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        super().accept(visitor)
        visitor.visit_set(self)
        self.right.accept(visitor)


class Aggregation(collections.namedtuple('Aggregation', 'columns, condition')):
    """GroupBy spec.
    """
    def __new__(cls, columns: typing.Iterable[schemamod.Column],
                condition: typing.Optional[schemamod.Condition] = None):
        return super().__new__(cls, tuple(columns), condition)


class Ordering(collections.namedtuple('Ordering', 'columns, direction')):
    """OrderBy spec.
    """
    @enum.unique
    class Direction(enum.Enum):
        """Ordering direction.
        """
        ASCENDING = 'ascending'
        DESCENDING = 'descending'

    def __new__(cls, columns: typing.Sequence[schemamod.Column], direction: 'Ordering.Direction' = Direction.ASCENDING):
        return super().__new__(cls, tuple(columns), direction)


class Rows(collections.namedtuple('Rows', 'count, offset')):
    """Row limit spec.
    """
    def __new__(cls, count: int, offset: int = 0):
        return super().__new__(cls, count, offset)


class Query(collections.namedtuple('Query', 'source, columns, condition, aggregation, ordering, rows'),
            schemamod.Source):
    """Generic source descriptor.
    """
    def __new__(cls, source: 'schemamod.Source', columns: typing.Optional[typing.Sequence[schemamod.Column]] = None,
                condition: typing.Optional[schemamod.Condition] = None,
                aggregation: typing.Optional[Aggregation] = None, ordering: typing.Optional[schemamod.Column] = None,
                rows: typing.Optional[Rows] = None):
        return super().__new__(cls, source, tuple(columns or []), condition, aggregation, ordering, rows)

    def accept(self, visitor: Visitor) -> None:
        self.source.accept(visitor)
        super().accept(visitor)
        visitor.visit_query(self)

    def select(self, columns: typing.Sequence[schemamod.Column]) -> 'Query':
        """Specify the output columns to be provided.
        """
        return Query(self.source, columns, self.condition, self.aggregation, self.ordering, self.rows)

    def filter(self, condition: schemamod.Condition) -> 'Query':
        """Add a row filtering condition.
        """
        return Query(self.source, self.columns, condition, self.aggregation, self.ordering, self.rows)

    def join(self, other: schemamod.Source, condition: schemamod.Condition,
             kind: Join.Kind = Join.Kind.LEFT) -> 'Query':
        """Join with other source.
        """
        return Query(Join(self.source, other, condition, kind), self.columns, self.condition, self.aggregation,
                     self.ordering, self.rows)

    def groupby(self, columns: typing.Iterable[schemamod.Column],
                condition: typing.Optional[schemamod.Condition] = None) -> 'Query':
        """Aggregating spec.
        """
        return Query(self.source, self.columns, self.condition, Aggregation(columns, condition), self.ordering,
                     self.rows)

    def orderby(self, columns: typing.Sequence[schemamod.Column]) -> 'Query':
        """Ordering spec.
        """

    def union(self, other: schemamod.Source) -> 'Query':
        """Set union with the other source.
        """
        return Query(Set(self, other, Set.Kind.UNION))

    def intersection(self, other: schemamod.Source) -> 'Query':
        """Set intersection with the other source.
        """
        return Query(Set(self, other, Set.Kind.INTERSECTION))

    def difference(self, other: schemamod.Source) -> 'Query':
        """Set difference with the other source.
        """
        return Query(Set(self, other, Set.Kind.DIFFERENCE))

    def limit(self, count: int, offset: int = 0) -> 'Query':
        """Restrict the result rows by its max count with an optional offset.
        """
        return Query(self.source, self.columns, self.condition, self.aggregation, self.ordering,
                     Rows(count, offset))
