"""
ETL statement types.
"""
import abc
import collections
import enum
import typing

from forml.etl.schema import kind, frame, series


class Visitor(frame.Visitor, metaclass=abc.ABCMeta):
    """Statement visitor.
    """
    def visit_join(self, source: 'Join') -> None:
        """Generic source hook.

        Args:
            source: frame.Source instance to be visited.
        """
        self.visit_source(source)

    def visit_set(self, source: 'Set') -> None:
        """Generic source hook.

        Args:
            source: frame.Source instance to be visited.
        """
        self.visit_source(source)

    def visit_query(self, source: 'Query') -> None:
        """Generic source hook.

        Args:
            source: frame.Source instance to be visited.
        """
        self.visit_source(source)


class Condition(collections.namedtuple('Condition', 'expression')):
    """Special type to wrap boolean expressions that can be used as conditions.
    """
    def __new__(cls, expression: series.Expression):
        if not isinstance(expression.kind, kind.Boolean):
            raise ValueError(f'{expression.kind} expression cannot be used as a condition')
        return super().__new__(cls, expression)


class Join(collections.namedtuple('Join', 'left, right, condition, kind'), frame.Source):
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

    def __new__(cls, left: frame.Source, right: frame.Source, condition: series.Expression,
                kind: typing.Optional['Join.Kind'] = None):
        return super().__new__(cls, left, right, Condition(condition), kind or cls.Kind.LEFT)

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_join(self)


class Set(collections.namedtuple('Set', 'left, right, kind'), frame.Source):
    """Source made of two set-combined subsources.
    """
    @enum.unique
    class Kind(enum.Enum):
        """Set type.
        """
        UNION = 'union'
        INTERSECTION = 'intersection'
        DIFFERENCE = 'difference'

    def __new__(cls, left: frame.Source, right: frame.Source, kind: 'Set.Kind'):
        return super().__new__(cls, left, right, kind)

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_set(self)


class Grouping(collections.namedtuple('Grouping', 'columns, condition')):
    """GroupBy spec.

    The condition parameter is an optional expression equivalent to the classical HAVING clause.
    """
    def __new__(cls, columns: typing.Union[series.Column, typing.Iterable[series.Column]],
                condition: typing.Optional[series.Expression] = None):
        if isinstance(columns, series.Column):
            columns = [columns]
        if condition:
            condition = Condition(condition)
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

    def __new__(cls, columns: typing.Union[series.Column, typing.Sequence[series.Column]],
                direction: typing.Optional['Ordering.Direction'] = None):
        if isinstance(columns, series.Column):
            columns = [columns]
        return super().__new__(cls, tuple(columns), direction or cls.Direction.ASCENDING)


class Rows(collections.namedtuple('Rows', 'count, offset')):
    """Row limit spec.
    """
    def __new__(cls, count: int, offset: int = 0):
        return super().__new__(cls, count, offset)


class Query(collections.namedtuple('Query', 'source, columns, condition, grouping, ordering, rows'),
            frame.Queryable):
    """Generic source descriptor.
    """
    def __new__(cls, source: 'frame.Source',
                columns: typing.Optional[typing.Union[series.Column, typing.Sequence[series.Column]]] = None,
                condition: typing.Optional[Condition] = None, grouping: typing.Optional[Grouping] = None,
                ordering: typing.Optional[series.Column] = None, rows: typing.Optional[Rows] = None):
        if isinstance(columns, series.Column):
            columns = [columns]
        return super().__new__(cls, source, tuple(columns or []), condition, grouping, ordering, rows)

    def accept(self, visitor: Visitor) -> None:
        self.source.accept(visitor)
        visitor.visit_query(self)

    def select(self, columns: typing.Union[series.Column, typing.Sequence[series.Column]]) -> 'Query':
        return Query(self.source, columns, self.condition, self.grouping, self.ordering, self.rows)

    def filter(self, condition: series.Expression) -> 'Query':
        return Query(self.source, self.columns, Condition(condition), self.grouping, self.ordering, self.rows)

    def join(self, other: frame.Source, condition: series.Expression,
             kind: typing.Optional[Join.Kind] = None) -> 'Query':
        return Query(Join(self.source, other, condition, kind), self.columns, self.condition, self.grouping,
                     self.ordering, self.rows)

    def groupby(self, columns: typing.Union[series.Column, typing.Iterable[series.Column]],
                condition: typing.Optional[series.Expression] = None) -> 'Query':
        return Query(self.source, self.columns, self.condition, Grouping(columns, condition), self.ordering, self.rows)

    def orderby(self, columns: typing.Union[series.Column, typing.Sequence[series.Column]],
                direction: typing.Optional[Ordering.Direction] = None) -> 'Query':
        return Query(self.source, self.columns, self.condition, self.grouping, Ordering(columns, direction), self.rows)

    def limit(self, count: int, offset: int = 0) -> 'Query':
        return Query(self.source, self.columns, self.condition, self.grouping, self.ordering, Rows(count, offset))
