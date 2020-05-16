"""
ETL DSL compiler.
"""
import abc
import functools
import typing

from forml.etl.dsl import statement
from forml.etl.dsl.schema import frame, series


TargetT = typing.TypeVar('TargetT')


class Transpiler(typing.Generic[TargetT], statement.Visitor, series.Visitor, metaclass=abc.ABCMeta):
    """Transpiler interface implemented as a statement/series visitor.
    """
    def __init__(self, tables: typing.Mapping[frame.Table, TargetT]):
        self._tables: typing.Mapping[frame.Table, TargetT] = tables
        self._stack: typing.List[TargetT] = list()

    @property
    def target(self) -> TargetT:
        """Return the target code after this transpiler instance has be accepted by a statement.

        Returns: Target code of given statement.
        """
        assert len(self._stack) == 1, 'Unexpected transpiler state'
        return self._stack[0]

    @functools.lru_cache()
    def generate_column(self, column: series.Column) -> TargetT:
        """Generate target code for the generic column type.

        Args:
            column: Column instance

        Returns: Column in target code.
        """
        column.accept(self)
        return self._stack.pop()

    def visit_join(self, source: statement.Join) -> None:
        right = self._stack.pop()
        left = self._stack.pop()
        expression = self.generate_column(source.condition)
        self._stack.append(self.generate_join(left, right, expression, source.kind))

    def visit_set(self, source: statement.Set) -> None:
        right = self._stack.pop()
        left = self._stack.pop()
        self._stack.append(self.generate_set(left, right, source.kind))

    def visit_query(self, query: statement.Query) -> None:
        def mkcol(col: series.Column) -> TargetT:
            val = self.generate_column(col)
            if isinstance(col, series.Aliased):
                val = self.generate_alias(val, col.name)
            return val
        source = self._stack.pop()
        columns = [mkcol(c) for c in query.columns]
        where = self.generate_column(query.prefilter) if query.prefilter is not None else None
        groupby = [self.generate_column(c) for c in query.grouping]
        having = self.generate_column(query.postfilter) if query.postfilter is not None else None
        orderby = [self.generate_ordering(self.generate_column(c), o) for c, o in query.ordering]
        self._stack.append(self.generate_query(source, columns, where, groupby, having, orderby, query.rows))

    def visit_table(self, table: frame.Table) -> None:
        self._stack.append(self.generate_table(table))

    def generate_table(self, table: frame.Table) -> TargetT:
        """Generate target code for a table type.

        Args:
            table: Table instance.

        Returns: Target code for the table instance.
        """
        return self._tables[table]

    @abc.abstractmethod
    def generate_join(self, left: TargetT, right: TargetT, condition: TargetT, kind: statement.Join.Kind) -> TargetT:
        """Generate target code for a join operation using the left/right terms, given condition and a join type.

        Args:
            left: Left side of the join pair.
            right: Right side of the join pair.
            condition: Join condition.
            kind: Join type.

        Returns: Target code for the join operation.
        """

    @abc.abstractmethod
    def generate_set(self, left: TargetT, right: TargetT, kind: statement.Set.Kind) -> TargetT:
        """Generate target code for a set operation using the left/right terms, given a set type.

        Args:
            left: Left side of the set pair.
            right: Right side of the set pair.
            kind: Set type.

        Returns: Target code for the set operation.
        """

    def visit_field(self, field: series.Field) -> None:
        self._stack.append(self.generate_field(field))

    def visit_literal(self, literal: series.Literal) -> None:
        self._stack.append(self.generate_literal(literal))

    def visit_expression(self, expression: series.Expression) -> None:
        arguments = tuple(reversed(self._stack.pop() for _ in expression))
        self._stack.append(self.generate_expression(expression.__class__, arguments))

    @abc.abstractmethod
    def generate_field(self, field: series.Field) -> TargetT:
        """Generate target code for a field value.

        Args:
            field: Schema field instance.

        Returns: Field in target code representation.
        """

    @abc.abstractmethod
    def generate_literal(self, literal: series.Literal) -> TargetT:
        """Generate target code for a literal value.

        Args:
            literal: Literal value instance.

        Returns: Literal in target code representation.
        """

    @abc.abstractmethod
    def generate_expression(self, expression: typing.Type[series.Expression],
                            arguments: typing.Sequence[TargetT]) -> TargetT:
        """Generate target code for an expression of given arguments.

        Args:
            expression: Operator or function implementing the expression.
            arguments: Expression arguments.

        Returns: Expression in target code representation.
        """

    @abc.abstractmethod
    def generate_alias(self, column: TargetT, alias: str) -> TargetT:
        """Generate column alias code.

        Args:
            column: Column value already in target code.
            alias: Alias to be used for given column.

        Returns: Aliased column in target code.
        """

    @abc.abstractmethod
    def generate_ordering(self, column: TargetT, direction: statement.Ordering.Direction) -> TargetT:
        """Generate column ordering code.

        Args:
            column: Column value already in target code.
            direction: Ordering direction spec.

        Returns: Column ordering in target code.
        """

    @abc.abstractmethod
    def generate_query(self, source: TargetT, columns: typing.Sequence[TargetT],
                       where: typing.Optional[TargetT], groupby: typing.Sequence[TargetT],
                       having: typing.Optional[TargetT], orderby: typing.Sequence[TargetT],
                       rows: typing.Optional[statement.Rows]) -> TargetT:
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
