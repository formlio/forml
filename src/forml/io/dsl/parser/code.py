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
from typing import Any

from forml.io.dsl import parser as parsmod
from forml.io.dsl.schema import frame as framod, series as sermod

LOGGER = logging.getLogger(__name__)


Input = typing.TypeVar('Input')
Output = typing.TypeVar('Output')
Table = typing.TypeVar('Table', bound=typing.Sized)
Column = typing.TypeVar('Column', bound=typing.Sized)


class Closure(typing.Generic[Input, Output], tuple, metaclass=abc.ABCMeta):  # pylint: disable=unsubscriptable-object
    """Generic closure to be used as the parsing products.
    """
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

        Returns: Sequence of parameters for the closure handler.
        """

    def __call__(self, data: Input) -> Output:
        return self.handler(*self.__args__(data))


Tabulizer = Closure[Table, Table]  # pylint: disable=unsubscriptable-object
Columnizer = Closure[Table, Column]  # pylint: disable=unsubscriptable-object


class Frame(parsmod.Frame[Tabulizer, Columnizer], metaclass=abc.ABCMeta):
    """DSL parser producing an chain of table producing lambda statements.
    """
    class Join(Tabulizer):
        """Closure with parameters required for performing a join.
        """
        left: Tabulizer = property(operator.itemgetter(1))
        right: Tabulizer = property(operator.itemgetter(2))
        condition: typing.Optional[Columnizer] = property(operator.itemgetter(3))
        kind: framod.Join.Kind = property(operator.itemgetter(4))

        def __new__(cls, handler: typing.Callable[[Table, Table, typing.Optional[Columnizer], framod.Join.Kind], Table],
                    left: Tabulizer, right: Tabulizer, condition: typing.Optional[Columnizer],
                    kind: framod.Join.Kind) -> Any:
            return super().__new__(cls, handler, left, right, condition, kind)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.left(data), self.right(data), self.condition, self.kind

    class Set(Tabulizer):
        """Closure with parameters required for performing a set.
        """
        left: Tabulizer = property(operator.itemgetter(1))
        right: Tabulizer = property(operator.itemgetter(2))
        kind: framod.Set.Kind = property(operator.itemgetter(3))

        def __new__(cls, handler: typing.Callable[[Table, Table, framod.Set.Kind], Table],
                    left: Tabulizer, right: Tabulizer, kind: framod.Set.Kind) -> Any:
            return super().__new__(cls, handler, left, right, kind)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.left(data), self.right(data), self.kind

    class Query(Tabulizer):
        """Closure with parameters required for performing a query.
        """
        source: Tabulizer = property(operator.itemgetter(1))
        columns: typing.Sequence[Columnizer] = property(operator.itemgetter(2))
        where: typing.Optional[Columnizer] = property(operator.itemgetter(3))
        groupby: typing.Sequence[Columnizer] = property(operator.itemgetter(4))
        having: typing.Optional[Columnizer] = property(operator.itemgetter(5))
        orderby: Tabulizer = property(operator.itemgetter(6))
        rows: typing.Optional[framod.Rows] = property(operator.itemgetter(7))

        def __new__(cls, handler: typing.Callable[[Table, typing.Sequence[Columnizer], typing.Optional[Columnizer],
                                                   typing.Sequence[Columnizer], typing.Optional[Columnizer],
                                                   typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
                                                   typing.Optional[framod.Rows]], Table],
                    source: Tabulizer, columns: typing.Sequence[Columnizer], where: typing.Optional[Columnizer],
                    groupby: typing.Sequence[Columnizer], having: typing.Optional[Columnizer],
                    orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
                    rows: typing.Optional[framod.Rows]) -> Any:
            return super().__new__(cls, handler, source, tuple(columns), where, tuple(groupby), having, orderby, rows)

        def __args__(self, data: Table) -> typing.Sequence[typing.Any]:
            return self.source(data), self.columns, self.where, self.groupby, self.having, self.orderby, self.rows

    @abc.abstractmethod
    def implement_join(self, left: Table, right: Table, condition: typing.Optional[Columnizer],
                       kind: framod.Join.Kind) -> Table:
        """Implementation of join of two tables.

        Args:
            left: Left table to be joined.
            right: Right table to be joined.
            condition: Joining condition.
            kind: Join type (left outer, right outer, full outer, inner, cross).

        Returns: Joined table.
        """

    @abc.abstractmethod
    def implement_set(self, left: Table, right: Table, kind: framod.Set.Kind) -> Table:
        """Implementation of set combination of two tables with same columns.

        Args:
            left: Left side of the set operation.
            right: Right side of the set operation.
            kind: Type of the set operation (union/intersection/difference).

        Returns: Combined table.
        """

    @abc.abstractmethod
    def implement_apply(self, data: Table, partition: typing.Optional[typing.Sequence[Columnizer]] = None,
                        evaluation: typing.Sequence[typing.Sequence[Columnizer]] = None,
                        predicate: typing.Optional[Columnizer] = None) -> Table:
        """Split the input data into groups based on the specified grouping columns,
        then discard all groups for which condition is not met and finally evaluate and materialize given expression
        producing new data consisting of single record per each (remaining) group.

        Args:
            data: Input data to be transformed.
            partition: Optional set of expressions defining the data grouping.
            evaluation: Optional set of expressions producing new columns upon evaluation (might be aggregating in which
                        case the new columns should extend the set of the partitioning columns).
            predicate: Optional filter to be applied on the evaluated data.

        Returns: Transformed dataset.
        """

    @abc.abstractmethod
    def implement_ordering(self, data: Table,
                           specs: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]]) -> Table:
        """Row ordering implementation.

        Args:
            data: Input table.
            specs: Ordering specs - tuple of column and direction specifying the ordering.

        Returns: Table with rows ordered according to given specs.
        """

    @abc.abstractmethod
    def implement_project(self, data: Table, columns: typing.Sequence[Columnizer]) -> Table:
        """Column projection implementation.

        Args:
            data: Input table.
            columns: Columns to be projected.

        Returns: Table with the exact columns in given order.
        """

    @abc.abstractmethod
    def implement_limit(self, data: Table, count: int, offset: int) -> Table:
        """Count based row limitation implementation.

        Args:
            data: Input table.
            count: Number of rows to limit the result to.
            offset: Number of rows to skip from the beginning of the input table.

        Returns: Table with the limited row count.
        """

    def implement_query(self, table: Table, columns: typing.Sequence[Columnizer], where: typing.Optional[Columnizer],
                        groupby: typing.Sequence[Columnizer], having: typing.Optional[Columnizer],
                        orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
                        rows: typing.Optional[framod.Rows]) -> Table:
        if where is not None:
            table = self.implement_apply(table, predicate=where)
        if groupby:
            # adding orderby columns as they might be aggregation based
            _seen = set(groupby)
            aggregate = tuple(c for c in itertools.chain(columns, (o for o, _ in orderby))
                              if c not in _seen and not _seen.add(c))
            table = self.implement_apply(table, partition=groupby, evaluation=aggregate, predicate=having)
        if orderby:
            table = self.implement_ordering(table, orderby)
        if columns:
            table = self.implement_project(table, columns)
        if rows:
            table = self.implement_limit(table, rows.count, rows.offset)
        return table

    def generate_join(self, left: Tabulizer, right: Tabulizer, condition: typing.Optional[Columnizer],
                      kind: framod.Join.Kind) -> 'Frame.Join':
        return self.Join(self.implement_join, left, right, condition, kind)

    def generate_set(self, left: Tabulizer, right: Tabulizer, kind: framod.Set.Kind) -> 'Frame.Set':
        return self.Set(self.implement_set, left, right, kind)

    def generate_query(self, source: Tabulizer, columns: typing.Sequence[Columnizer],
                       where: typing.Optional[Columnizer], groupby: typing.Sequence[Columnizer],
                       having: typing.Optional[Columnizer],
                       orderby: typing.Sequence[typing.Tuple[Columnizer, sermod.Ordering.Direction]],
                       rows: typing.Optional[framod.Rows]) -> 'Frame.Query':
        return self.Query(self.implement_query, source, columns, where, groupby, having, orderby, rows)

    def generate_reference(self, instance: Tabulizer, name: str) -> Tabulizer:
        pass


class Series(Frame[Tabulizer, Columnizer], parsmod.Series[Columnizer, Columnizer]):
    """DSL parser producing a chain of column producing lambda statements.
    """
    def generate_field(self, source: Columnizer, field: Columnizer) -> Columnizer:
        pass

    def generate_alias(self, column: Columnizer, alias: str) -> Columnizer:
        pass

    def generate_literal(self, literal: sermod.Literal) -> Columnizer:
        pass

    def generate_expression(self, expression: typing.Type[sermod.Column],
                            arguments: typing.Sequence[Columnizer]) -> Columnizer:
        pass
