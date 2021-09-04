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
Code parser tests.
"""
# pylint: disable=no-self-use
import collections
import operator
import types
import typing

import pytest

from forml.io.dsl.parser import code
from forml.io.dsl.parser.code import Columnizer
from forml.io.dsl.struct import series as sermod, frame as framod, kind as kindmod
from . import TupleParser


class TestClosure:
    """Closure unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def handler() -> typing.Callable[[tuple, tuple], typing.Tuple[tuple, tuple]]:
        """Handler fixture."""
        return lambda a, b: (a, b)

    @staticmethod
    @pytest.fixture(scope='session')
    def arg() -> tuple:
        """Arg fixture."""
        return 'foo', 'bar'

    @staticmethod
    @pytest.fixture(scope='session')
    def closure(handler: typing.Callable[[tuple, tuple], typing.Tuple[tuple, tuple]], arg: tuple):
        """Closure fixture."""

        class Closure(code.Closure[tuple, tuple]):  # pylint: disable=unsubscriptable-object
            """Test closure implementation."""

            arg: tuple = property(operator.itemgetter(1))

            def __new__(cls, handler_: typing.Callable[[tuple, tuple], typing.Tuple[tuple, tuple]], arg_: tuple):
                return super().__new__(cls, handler_, arg_)

            def __args__(self, data: tuple) -> typing.Sequence[typing.Any]:
                return data, self.arg

        return Closure(handler, arg)

    def test_call(
        self,
        closure: code.Closure[tuple, tuple],  # pylint: disable=unsubscriptable-object
        handler: typing.Callable[[tuple, tuple], typing.Tuple[tuple, tuple]],
        arg: tuple,
    ):
        """Closure call test."""
        assert closure(('b', 'a')) == handler(('b', 'a'), arg)

    def test_identity(self, closure):
        """Test the identity (hashability + equality)."""
        assert len({closure, closure}) == 1


class Frame(code.Frame[tuple, tuple]):  # pylint: disable=unsubscriptable-object
    """Dummy frame parser wrapping all terms into tuples."""

    class Result(collections.namedtuple('Result', 'source, columns, where, groupby, having, orderby, rows')):
        """Query result helper tuple."""

        NONE = tuple()

        def __new__(
            cls,
            source: typing.Optional[tuple] = None,
            columns: typing.Optional[tuple] = None,
            where: typing.Optional[tuple] = None,
            groupby: typing.Optional[tuple] = None,
            having: typing.Optional[tuple] = None,
            orderby: typing.Optional[tuple] = None,
            rows: typing.Optional[tuple] = None,
        ):
            if isinstance(source, cls):
                columns = columns or source.columns
                where = where or source.where
                groupby = groupby or source.groupby
                having = having or source.having
                orderby = orderby or source.orderby
                rows = rows or source.rows
                source = source.source
            return super().__new__(
                cls,
                source or cls.NONE,
                columns or cls.NONE,
                where or cls.NONE,
                groupby or cls.NONE,
                having or cls.NONE,
                orderby or cls.NONE,
                rows or cls.NONE,
            )

    class Series(code.Frame.Series[tuple, tuple]):
        """Dummy series parser wrapping all terms into tuples."""

        # pylint: disable=missing-function-docstring
        def implement_element(self, data: tuple, column: Columnizer) -> tuple:
            return data, column(data)

        def implement_alias(self, data: tuple, column: code.Columnizer, name: str) -> tuple:
            return column(data), name

        def implement_literal(self, value: typing.Any, kind: kindmod.Any) -> tuple:
            return value, kind

        def implement_expression(
            self, expression: typing.Type[sermod.Expression], arguments: typing.Sequence[typing.Any]
        ) -> tuple:
            return expression, *arguments

    # pylint: disable=missing-function-docstring
    def implement_join(
        self, left: tuple, right: tuple, condition: typing.Optional[code.Columnizer], kind: framod.Join.Kind
    ) -> tuple:
        return left, kind, right, condition(left)

    def implement_set(self, left: tuple, right: tuple, kind: framod.Set.Kind) -> tuple:
        return left, kind, right

    def implement_apply(
        self,
        table: tuple,
        partition: typing.Optional[typing.Sequence[code.Columnizer]] = None,
        expression: typing.Optional[typing.Sequence[code.Columnizer]] = None,
        predicate: typing.Optional[code.Columnizer] = None,
    ) -> tuple:
        where = having = None
        if predicate:
            predicate = predicate(table)
        if partition:
            partition = tuple(p(table) for p in partition)
            having = predicate
        else:
            where = predicate
        if expression:
            expression = tuple(e(table) for e in expression)
        return self.Result(table, expression, where, partition, having)

    def implement_ordering(
        self, table: tuple, specs: typing.Sequence[typing.Tuple[code.Columnizer, sermod.Ordering.Direction]]
    ) -> tuple:
        return self.Result(table, orderby=tuple((s(table), o) for s, o in specs))

    def implement_project(self, table: tuple, columns: typing.Sequence[code.Columnizer]) -> tuple:
        return self.Result(table, columns=tuple(c(table) for c in columns))

    def implement_limit(self, table: tuple, count: int, offset: int) -> tuple:
        return self.Result(table, rows=(count, offset))

    def implement_reforigin(self, table: tuple, name: str) -> tuple:
        return table, name

    def implement_refhandle(self, name: str) -> tuple:
        return tuple([name])


class TestParser(TupleParser):
    """Code parser tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(
        person: framod.Table, student: framod.Table, school: framod.Table
    ) -> typing.Mapping[framod.Source, typing.Callable[[tuple], tuple]]:
        """Sources mapping fixture."""
        return types.MappingProxyType(
            {
                framod.Join(student, person, student.surname == person.surname): lambda _: tuple(['foo']),
                person: lambda _: tuple([person]),
                student: lambda _: tuple([student]),
                school: lambda _: tuple([school]),
            }
        )

    @staticmethod
    @pytest.fixture(scope='session')
    def columns(student: framod.Table) -> typing.Mapping[sermod.Column, typing.Callable[[tuple], tuple]]:
        """Columns mapping fixture."""

        class Columns:
            """Columns mapping."""

            def __getitem__(self, column: sermod.Column) -> typing.Callable[[tuple], tuple]:
                if column == student.level:
                    return lambda _: tuple(['baz'])
                if isinstance(column, sermod.Element):
                    return lambda _: tuple([column])
                raise KeyError('Unknown column')

        return Columns()

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(
        sources: typing.Mapping[framod.Source, typing.Callable[[tuple], tuple]],
        columns: typing.Mapping[sermod.Column, typing.Callable[[tuple], tuple]],
    ) -> Frame:
        """Parser fixture."""
        return Frame(sources, columns)

    def format(self, result: code.Tabulizer) -> tuple:
        return result(None)
