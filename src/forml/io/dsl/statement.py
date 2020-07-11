"""
ETL statement types.
"""
import abc
import collections
import enum
import itertools
import typing
from collections import abc as colabc

from forml.io.dsl.schema import series, frame


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
                kind: typing.Optional[typing.Union['Join.Kind', str]] = None):
        return super().__new__(cls, left, right, series.Logical.ensure(series.Element.ensure(condition)),
                               cls.Kind(kind) if kind else cls.Kind.LEFT)

    @property
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

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

    @property
    def columns(self) -> typing.Sequence[series.Column]:
        return self.left.columns + self.right.columns

    def accept(self, visitor: Visitor) -> None:
        self.left.accept(visitor)
        self.right.accept(visitor)
        visitor.visit_set(self)


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


class Query(collections.namedtuple('Query', 'source, selection, prefilter, grouping, postfilter, ordering, rows'),
            frame.Queryable):
    """Generic source descriptor.
    """
    def __new__(cls, source: 'frame.Source',
                selection: typing.Optional[typing.Iterable[series.Column]] = None,
                prefilter: typing.Optional[series.Expression] = None,
                grouping: typing.Optional[typing.Iterable[series.Element]] = None,
                postfilter: typing.Optional[series.Expression] = None,
                ordering: typing.Optional[typing.Sequence[typing.Union[series.Element,
                                                                       typing.Union['Ordering.Direction', str],
                                                                       typing.Tuple[series.Element, typing.Union[
                                                                           'Ordering.Direction', str]]]]] = None,
                rows: typing.Optional[Rows] = None):
        def fields(columns: typing.Iterable[series.Column]) -> typing.Set[series.Field]:
            """Extract true Field instances from given set of columns.

            Args:
                columns: Input set of columns to extract the fields from.

            Returns: Set of extracted fields.
            """
            return {f for c in columns for f in series.Field.disect(series.Column.ensure(c))}

        if selection and fields(selection).difference(fields(source.columns)):
            raise ValueError(f'Selection ({selection}) is not a subset of source columns ({source.columns})')
        if prefilter is not None:
            series.Logical.ensure(series.Element.ensure(prefilter))
        if postfilter is not None:
            series.Logical.ensure(series.Element.ensure(postfilter))
        return super().__new__(cls, source, tuple(selection or []), prefilter,
                               tuple(series.Element.ensure(g) for g in grouping or []), postfilter,
                               tuple(Ordering.make(ordering or [])), rows)

    @property
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

    def join(self, other: frame.Source, condition: series.Expression,
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
