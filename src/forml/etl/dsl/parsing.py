"""
ETL DSL parser.
"""
import abc
import functools
import logging
import types
import typing

from forml.etl.dsl import statement, error
from forml.etl.dsl.schema import frame, series

LOGGER = logging.getLogger(__name__)
ResultT = typing.TypeVar('ResultT')


def bypass(override: typing.Callable[['Visitor', typing.Any], None]) -> typing.Callable:
    """By pass the (result of) the particular visit_* implementation if the supplied override resolver provides an
    alternative value.

    Args:
        override: Callable resolver that returns an explicit value for given subject or raises KeyError for unknown
        mapping.

    Returns: Visitor method decorator.
    """
    def decorator(visit: typing.Callable[['Visitor', typing.Any], None]) -> typing.Callable:
        """Visitor method decorator with added bypassing capability.

        Args:
            visit: Visitor method to be decorated.

        Returns: Decorated version of the visit_* method.
        """
        @functools.wraps(visit)
        def wrapped(self: 'Visitor', subject: typing.Any) -> None:
            """Decorated version of the visit_* method.

            Args:
                self: Visitor instance.
                subject: Visited subject.
            """
            visit(self, subject)
            try:
                result = override(self, subject)
            except error.Mapping:
                pass
            else:
                LOGGER.debug('Overriding result for %s', subject)
                # pylint: disable=protected-access
                self._stack.pop()
                self._stack.append(result)

        return wrapped
    return decorator


class Visitor(typing.Generic[ResultT], statement.Visitor, series.Visitor, metaclass=abc.ABCMeta):
    """Parsing interface implemented as a statement/series visitor.
    """
    def __init__(self, sources: typing.Mapping[frame.Source, ResultT], columns: typing.Mapping[series.Column, ResultT]):
        self._sources: typing.Mapping[frame.Source, ResultT] = types.MappingProxyType(sources)
        self._columns: typing.Mapping[series.Column, ResultT] = types.MappingProxyType(columns)
        self._stack: typing.List[ResultT] = list()

    @property
    def result(self) -> ResultT:
        """Return the target code after this visitor instance has been accepted by a statement.

        Returns: Target code of given statement.
        """
        assert len(self._stack) == 1, 'Unexpected visitor state'
        return self._stack[0]

    @functools.lru_cache()
    def generate_column(self, column: series.Column) -> ResultT:
        """Generate target code for the generic column type.

        Args:
            column: Column instance

        Returns: Column in target code.
        """
        column.accept(self)
        return self._stack.pop()

    def generate_table(self, table: frame.Table) -> ResultT:
        """Generate target code for a table type.

        Args:
            table: Table instance.

        Returns: Target code for the table instance.
        """
        try:
            return self._sources[table]
        except KeyError:
            raise error.Mapping(f'Unknown mapping for table {table}')

    @abc.abstractmethod
    def generate_join(self, left: ResultT, right: ResultT, condition: ResultT, kind: statement.Join.Kind) -> ResultT:
        """Generate target code for a join operation using the left/right terms, given condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns: Target code for the join operation.
        """

    @abc.abstractmethod
    def generate_set(self, left: ResultT, right: ResultT, kind: statement.Set.Kind) -> ResultT:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns: Target code for the set operation.
        """

    @abc.abstractmethod
    def generate_query(self, source: ResultT, columns: typing.Sequence[ResultT],
                       where: typing.Optional[ResultT], groupby: typing.Sequence[ResultT],
                       having: typing.Optional[ResultT], orderby: typing.Sequence[ResultT],
                       rows: typing.Optional[statement.Rows]) -> ResultT:
        """Generate query statement code.

        Args:
            source: Source already in target code.
            columns: Sequence of selected columns in target code.
            where: Where condition in target code.
            groupby: Sequence of grouping specifiers in target code.
            having: Having condition in target code.
            orderby: Sequence of ordering specifiers in target code.
            rows: Limit spec tuple.

        Returns: Query in target code.
        """

    @abc.abstractmethod
    def generate_alias(self, column: ResultT, alias: str) -> ResultT:
        """Generate column alias code.

        Args:
            column: Column value already in target code.
            alias: Alias to be used for given column.

        Returns: Aliased column in target code.
        """

    @abc.abstractmethod
    def generate_ordering(self, column: ResultT, direction: statement.Ordering.Direction) -> ResultT:
        """Generate column ordering code.

        Args:
            column: Column value already in target code.
            direction: Ordering direction spec.

        Returns: Column ordering in target code.
        """

    def visit_table(self, source: frame.Table) -> None:
        self._stack.append(self.generate_table(source))

    @bypass(generate_table)
    def visit_join(self, source: statement.Join) -> None:
        right = self._stack.pop()
        left = self._stack.pop()
        expression = self.generate_column(source.condition)
        self._stack.append(self.generate_join(left, right, expression, source.kind))

    @bypass(generate_table)
    def visit_set(self, source: statement.Set) -> None:
        right = self._stack.pop()
        left = self._stack.pop()
        self._stack.append(self.generate_set(left, right, source.kind))

    @bypass(generate_table)
    def visit_query(self, source: statement.Query) -> None:
        def mkcol(col: series.Column) -> ResultT:
            val = self.generate_column(col)
            if isinstance(col, series.Aliased):
                val = self.generate_alias(val, col.name)
            return val
        columns = [mkcol(c) for c in source.columns]
        where = self.generate_column(source.prefilter) if source.prefilter is not None else None
        groupby = [self.generate_column(c) for c in source.grouping]
        having = self.generate_column(source.postfilter) if source.postfilter is not None else None
        orderby = [self.generate_ordering(self.generate_column(c), o) for c, o in source.ordering]
        self._stack.append(self.generate_query(self._stack.pop(), columns, where, groupby, having, orderby,
                                               source.rows))

    def generate_field(self, field: series.Field) -> ResultT:
        """Generate target code for a field value.

        Args:
            field: Schema field instance.

        Returns: Field in target code representation.
        """
        try:
            return self._columns[field]
        except KeyError:
            raise error.Mapping(f'Unknown mapping for field {field}')

    @abc.abstractmethod
    def generate_literal(self, literal: series.Literal) -> ResultT:
        """Generate target code for a literal value.

        Args:
            literal: Literal value instance.

        Returns: Literal in target code representation.
        """

    @abc.abstractmethod
    def generate_expression(self, expression: typing.Type[series.Expression],
                            arguments: typing.Sequence[ResultT]) -> ResultT:
        """Generate target code for an expression of given arguments.

        Args:
            expression: Operator or function implementing the expression.
            arguments: Expression arguments.

        Returns: Expression in target code representation.
        """

    def visit_field(self, column: series.Field) -> None:
        self._stack.append(self.generate_field(column))

    @bypass(generate_field)
    def visit_literal(self, column: series.Literal) -> None:
        self._stack.append(self.generate_literal(column))

    @bypass(generate_field)
    def visit_expression(self, column: series.Expression) -> None:
        arguments = tuple(reversed([self._stack.pop() for _ in column]))
        self._stack.append(self.generate_expression(column.__class__, arguments))
