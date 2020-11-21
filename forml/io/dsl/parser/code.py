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
DSL parser into chain of executable lambdas.
"""
import abc
import itertools
import logging
import operator
import typing

from forml.io.dsl import parser as parsmod
from forml.io.dsl.struct import frame as framod, series as sermod, kind as kindmod

LOGGER = logging.getLogger(__name__)


Input = typing.TypeVar('Input')
Output = typing.TypeVar('Output')
Table = typing.TypeVar('Table', bound=typing.Sized)
Column = typing.TypeVar('Column', bound=typing.Sized)


class Closure(typing.Generic[Input, Output], tuple, metaclass=abc.ABCMeta):  # pylint: disable=unsubscriptable-object
    """Generic closure to be used as the parsing products."""

    handler: typing.Callable[..., Output] = property(operator.itemgetter(0))

    def __new__(cls, handler: typing.Callable[..., Output], *args):
        return super().__new__(cls, [handler, *args])

    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    @abc.abstractmethod
    def __args__(self, data: Input) -> typing.Sequence[typing.Any]:
        """Get the list of arguments to be passed to the handler upon execution.

        Args:
            data: Input data.

        Returns:
            Sequence of parameters for the closure handler.
        """

    def __call__(self, data: Input, **kwargs) -> Output:
        return self.handler(*self.__args__(data), **kwargs)


Tabulizer = Closure[Table, Table]  # pylint: disable=unsubscriptable-object
Columnizer = Closure[Table, Column]  # pylint: disable=unsubscriptable-object


class Frame(parsmod.Frame[Tabulizer, Columnizer], metaclass=abc.ABCMeta):
    """DSL parser producing an chain of table producing lambda statements."""

    class Series(parsmod.Frame.Series[Columnizer, Columnizer]):
        """DSL parser producing a chain of column producing lambda statements."""

        class Element(Columnizer):
            """Closure with parameters required for retrieving a field."""

            source: Tabulizer = property(operator.itemgetter(1))
            column: Columnizer = property(operator.itemgetter(2))

            def __new__(
                cls, handler: typing.Callable[[Table, Columnizer], Column], source: Tabulizer, column: Columnizer
            ) -> Columnizer:
                return super().__new__(cls, handler, source, column)

            def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
                return self.source(data), self.column

        class Alias(Columnizer):
            """Closure with parameters required for creating a column alias."""

            column: Columnizer = property(operator.itemgetter(1))
            name: str = property(operator.itemgetter(2))

            def __new__(
                cls, handler: typing.Callable[[Table, Columnizer, str], Column], column: Columnizer, name: str
            ) -> Columnizer:
                return super().__new__(cls, handler, column, name)

            def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
                return data, self.column, self.name

        class Literal(Columnizer):
            """Closure with parameters required for creating a literal value."""

            value: typing.Any = property(operator.itemgetter(1))
            kind: kindmod.Any = property(operator.itemgetter(2))

            def __new__(
                cls, handler: typing.Callable[[typing.Any, kindmod.Any], Column], value: typing.Any, kind: kindmod.Any
            ) -> Columnizer:
                return super().__new__(cls, handler, value, kind)

            def __args__(self, _: Table) -> typing.Sequence[typing.Any]:
                return self.value, self.kind

        class Expression(Columnizer):
            """Closure with parameters required for creating an expression."""

            kind: typing.Type[sermod.Expression] = property(operator.itemgetter(1))
            arguments: typing.Tuple[typing.Any] = property(operator.itemgetter(2))

            def __new__(
                cls,
                handler: typing.Callable[[typing.Type[sermod.Expression], typing.Sequence[Column]], Column],
                kind: typing.Type[sermod.Expression],
                arguments: typing.Sequence[typing.Any],
            ) -> Columnizer:
                return super().__new__(cls, handler, kind, tuple(arguments))

            def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
                return self.kind, tuple(c(data) if isinstance(c, Closure) else c for c in self.arguments)

        class Reference(Columnizer):
            """Closure with parameters required for holding a reference."""

            name: str = property(operator.itemgetter(1))

            def __new__(cls, handler: typing.Callable[[Table, str], Table], name: str) -> Tabulizer:
                return super().__new__(cls, handler, name)

            def __args__(self, _: Table) -> typing.Sequence[typing.Any]:
                return tuple([self.name])

        def implement_element(self, data: Table, column: Columnizer) -> Column:  # pylint: disable=no-self-use
            """Column provider implementation.

            Args:
                data: Dataframe to get the column from.
                column: Column reference.

            Returns:
                Column instance.
            """
            return column(data)

        @abc.abstractmethod
        def implement_alias(self, data: Table, column: Columnizer, name: str) -> Column:
            """Column provider implementation.

            Args:
                data: Dataframe whose column is to be aliased.
                column: Column reference.
                name: Column alias.

            Returns:
                Aliased column instance.
            """

        @abc.abstractmethod
        def implement_literal(self, value: typing.Any, kind: kindmod.Any) -> Column:
            """Literal value provider implementation.

            Args:
                value: Literal value.
                kind: Value type.

            Returns:
                Literal value column instance.
            """

        @abc.abstractmethod
        def implement_expression(
            self, expression: typing.Type[sermod.Expression], arguments: typing.Sequence[typing.Any]
        ) -> Column:
            """Literal value provider implementation.

            Args:
                expression: Expression class.
                arguments: Sequence of expression arguments.

            Returns:
                Column as the expression evaluation.
            """

        @abc.abstractmethod
        def implement_reference(self, name: str) -> Column:
            """Reference value provider implementation.

            Args:
                name: Reference name.

            Returns:
                Column as the expression evaluation.
            """

        # pylint: disable=missing-function-docstring
        def generate_element(self, source: Tabulizer, element: Columnizer) -> Columnizer:
            return self.Element(self.implement_element, source, element)

        def generate_alias(self, column: Columnizer, alias: str) -> Columnizer:
            return self.Alias(self.implement_alias, column, alias)

        def generate_literal(self, value: typing.Any, kind: kindmod.Any) -> Columnizer:
            return self.Literal(self.implement_literal, value, kind)

        def generate_reference(self, name: str) -> Columnizer:
            return self.Reference(self.implement_reference, name)

        def generate_expression(
            self, expression: typing.Type[sermod.Expression], arguments: typing.Sequence[typing.Any]
        ) -> Columnizer:
            return self.Expression(self.implement_expression, expression, arguments)

    class Join(Tabulizer):
        """Closure with parameters required for performing a join."""

        left: Tabulizer = property(operator.itemgetter(1))
        right: Tabulizer = property(operator.itemgetter(2))
        condition: typing.Optional[Columnizer] = property(operator.itemgetter(3))
        kind: framod.Join.Kind = property(operator.itemgetter(4))

        def __new__(
            cls,
            handler: typing.Callable[[Table, Table, typing.Optional[Columnizer], framod.Join.Kind], Table],
            left: Tabulizer,
            right: Tabulizer,
            condition: typing.Optional[Columnizer],
            kind: framod.Join.Kind,
        ) -> Tabulizer:
            return super().__new__(cls, handler, left, right, condition, kind)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.left(data), self.right(data), self.condition, self.kind

    class Set(Tabulizer):
        """Closure with parameters required for performing a set."""

        left: Tabulizer = property(operator.itemgetter(1))
        right: Tabulizer = property(operator.itemgetter(2))
        kind: framod.Set.Kind = property(operator.itemgetter(3))

        def __new__(
            cls,
            handler: typing.Callable[[Table, Table, framod.Set.Kind], Table],
            left: Tabulizer,
            right: Tabulizer,
            kind: framod.Set.Kind,
        ) -> Tabulizer:
            return super().__new__(cls, handler, left, right, kind)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.left(data), self.right(data), self.kind

    class Query(Tabulizer):
        """Closure with parameters required for performing a query."""

        source: Tabulizer = property(operator.itemgetter(1))
        columns: typing.Sequence[Columnizer] = property(operator.itemgetter(2))
        where: typing.Optional[Columnizer] = property(operator.itemgetter(3))
        groupby: typing.Sequence[Columnizer] = property(operator.itemgetter(4))
        having: typing.Optional[Columnizer] = property(operator.itemgetter(5))
        orderby: Tabulizer = property(operator.itemgetter(6))
        rows: typing.Optional[framod.Rows] = property(operator.itemgetter(7))

        def __new__(
            cls,
            handler: typing.Callable[
                [
                    Table,
                    typing.Sequence[Columnizer],
                    typing.Optional[Columnizer],
                    typing.Sequence[Columnizer],
                    typing.Optional[Columnizer],
                    typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
                    typing.Optional[framod.Rows],
                ],
                Table,
            ],
            source: Tabulizer,
            columns: typing.Sequence[Columnizer],
            where: typing.Optional[Columnizer],
            groupby: typing.Sequence[Columnizer],
            having: typing.Optional[Columnizer],
            orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
            rows: typing.Optional[framod.Rows],
        ) -> Tabulizer:
            return super().__new__(cls, handler, source, tuple(columns), where, tuple(groupby), having, orderby, rows)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.source(data), self.columns, self.where, self.groupby, self.having, self.orderby, self.rows

    class Reference(Tabulizer):
        """Closure with parameters required for holding a reference."""

        instance: Tabulizer = property(operator.itemgetter(1))
        name: str = property(operator.itemgetter(2))

        def __new__(cls, handler: typing.Callable[[Table, str], Table], instance: Tabulizer, name: str) -> Tabulizer:
            return super().__new__(cls, handler, instance, name)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.instance(data), self.name

    @abc.abstractmethod
    def implement_join(
        self, left: Table, right: Table, condition: typing.Optional[Columnizer], kind: framod.Join.Kind
    ) -> Table:
        """Implementation of join of two tables.

        Args:
            left: Left table to be joined.
            right: Right table to be joined.
            condition: Joining condition.
            kind: Join type (left outer, right outer, full outer, inner, cross).

        Returns:
            Joined table.
        """

    @abc.abstractmethod
    def implement_set(self, left: Table, right: Table, kind: framod.Set.Kind) -> Table:
        """Implementation of set combination of two tables with same columns.

        Args:
            left: Left side of the set operation.
            right: Right side of the set operation.
            kind: Type of the set operation (union/intersection/difference).

        Returns:
            Combined table.
        """

    @abc.abstractmethod
    def implement_apply(
        self,
        table: Table,
        partition: typing.Optional[typing.Sequence[Columnizer]] = None,
        expression: typing.Optional[typing.Sequence[Columnizer]] = None,
        predicate: typing.Optional[Columnizer] = None,
    ) -> Table:
        """Split the input data into groups based on the specified grouping columns,
        then discard all groups for which condition is not met and finally evaluate and materialize given expression
        producing new data consisting of single record per each (remaining) group.

        Args:
            table: Input data to be transformed.
            partition: Optional set of expressions defining the data grouping.
            expression: Optional set of expressions producing new columns upon evaluation (might be aggregating in which
                        case the new columns should extend the set of the partitioning columns).
            predicate: Optional filter to be applied on the evaluated data.

        Returns:
            Transformed dataset.
        """

    @abc.abstractmethod
    def implement_ordering(
        self, table: Table, specs: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]]
    ) -> Table:
        """Row ordering implementation.

        Args:
            table: Input table.
            specs: Ordering specs - tuple of column and direction specifying the ordering.

        Returns:
            Table with rows ordered according to given specs.
        """

    @abc.abstractmethod
    def implement_project(self, table: Table, columns: typing.Sequence[Columnizer]) -> Table:
        """Column projection implementation.

        Args:
            table: Input table.
            columns: Columns to be projected.

        Returns:
            Table with the exact columns in given order.
        """

    @abc.abstractmethod
    def implement_limit(self, table: Table, count: int, offset: int) -> Table:
        """Count based row limitation implementation.

        Args:
            table: Input table.
            count: Number of rows to limit the result to.
            offset: Number of rows to skip from the beginning of the input table.

        Returns:
            Table with the limited row count.
        """

    def implement_query(
        self,
        table: Table,
        columns: typing.Sequence[Columnizer],
        where: typing.Optional[Columnizer],
        groupby: typing.Sequence[Columnizer],
        having: typing.Optional[Columnizer],
        orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
        rows: typing.Optional[framod.Rows],
    ) -> Table:
        """Query implementation.

        Args:
            table: Source to be queried.
            columns: List of columns for projection.
            where: Prefilter specifier.
            groupby: Grouping specifier.
            having: Postfilter specifier.
            orderby: Ordering specifier.
            rows: Row result limitation.

        Returns:
            Query result.
        """
        if where is not None:
            table = self.implement_apply(table, predicate=where)
        if groupby:
            # adding orderby columns as they might be aggregation based
            _seen = set(groupby)
            aggregate = tuple(
                c for c in itertools.chain(columns, (o for o, _ in orderby)) if c not in _seen and not _seen.add(c)
            )
            table = self.implement_apply(table, partition=groupby, expression=aggregate, predicate=having)
        if orderby:
            table = self.implement_ordering(table, orderby)
        if columns:
            table = self.implement_project(table, columns)
        if rows:
            table = self.implement_limit(table, rows.count, rows.offset)
        return table

    @abc.abstractmethod
    def implement_reference(self, table: Table, name: str) -> Table:
        """Table reference (alias) implementation.

        Args:
            table: Table to be referenced.
            name: Reference name.

        Returns:
            Referenced table.
        """

    # pylint: disable=missing-function-docstring
    def generate_join(
        self, left: Tabulizer, right: Tabulizer, condition: typing.Optional[Columnizer], kind: framod.Join.Kind
    ) -> 'Frame.Join':
        return self.Join(self.implement_join, left, right, condition, kind)

    def generate_set(self, left: Tabulizer, right: Tabulizer, kind: framod.Set.Kind) -> 'Frame.Set':
        return self.Set(self.implement_set, left, right, kind)

    def generate_query(
        self,
        source: Tabulizer,
        columns: typing.Sequence[Columnizer],
        where: typing.Optional[Columnizer],
        groupby: typing.Sequence[Columnizer],
        having: typing.Optional[Columnizer],
        orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
        rows: typing.Optional[framod.Rows],
    ) -> 'Frame.Query':
        return self.Query(self.implement_query, source, columns, where, groupby, having, orderby, rows)

    def generate_reference(self, instance: Tabulizer, name: str) -> Tabulizer:
        return self.Reference(self.implement_reference, instance, name)
