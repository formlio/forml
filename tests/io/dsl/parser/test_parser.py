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
ETL code statement tests.
"""
# pylint: disable=no-self-use
import types
import typing

import pytest

from forml.io.dsl import parser as parsmod
from forml.io.dsl.schema import series as sermod, frame as framod, kind as kindmod
from . import TupleParser


class TestStack:
    """Parser stack unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def stack() -> parsmod.Stack:
        """Stack fixture.
        """
        return parsmod.Stack()

    @staticmethod
    @pytest.fixture(scope='function')
    def value() -> object:
        """Value fixture.
        """
        return object()

    def test_usage(self, stack: parsmod.Stack, value: object):
        """Basic stack usage test.
        """
        stack.push(value)
        assert stack.pop() == value

    def test_context(self, stack: parsmod.Stack, value):
        """Test context nesting.
        """
        with stack:
            stack.push(value)
            with stack:
                with pytest.raises(IndexError):
                    stack.pop()
            assert stack.pop() == value
        with pytest.raises(RuntimeError):
            with stack:
                stack.push(value)


class Frame(parsmod.Frame[tuple, tuple]):  # pylint: disable=unsubscriptable-object
    """Dummy frame parser wrapping all terms into tuples.
    """
    # pylint: disable=missing-function-docstring
    def generate_join(self, left: tuple, right: tuple, condition: tuple, kind: framod.Join.Kind) -> tuple:
        return left, kind, right, condition

    def generate_set(self, left: tuple, right: tuple, kind: framod.Set.Kind) -> tuple:
        return left, kind, right

    def generate_query(self, source: tuple, columns: typing.Sequence[tuple], where: typing.Optional[tuple],
                       groupby: typing.Sequence[tuple], having: typing.Optional[tuple],
                       orderby: typing.Sequence[typing.Tuple[tuple, sermod.Ordering.Direction]],
                       rows: typing.Optional[framod.Rows]) -> tuple:
        return source, tuple(columns), where, tuple(groupby), having, tuple(orderby), rows

    def generate_reference(self, instance: tuple, name: str) -> tuple:
        return instance, name


class Series(Frame, parsmod.Series[tuple, tuple]):
    """Dummy series parser wrapping all terms into tuples.
    """
    # pylint: disable=missing-function-docstring
    def generate_field(self, source: tuple, field: tuple) -> tuple:
        return source, field

    def generate_literal(self, value: typing.Any, kind: kindmod.Any) -> tuple:
        return value, kind

    def generate_expression(self, expression: typing.Type[sermod.Expression],
                            arguments: typing.Sequence[typing.Any]) -> tuple:
        return expression, *arguments

    def generate_alias(self, column: tuple, alias: str) -> tuple:
        return column, alias

    def generate_reference(self, instance: tuple, name: str) -> tuple:
        return tuple([name])


@pytest.fixture(scope='session')
def sources(person: framod.Table, student: framod.Table, school: framod.Table) -> typing.Mapping[framod.Source, tuple]:
    """Sources mapping fixture.
    """
    return types.MappingProxyType({
        framod.Join(student, person, student.surname == person.surname): tuple(['foo']),
        person: tuple([person]),
        student: tuple([student]),
        school: tuple([school])
    })


@pytest.fixture(scope='session')
def columns() -> typing.Mapping[sermod.Column, tuple]:
    """Columns mapping fixture.
    """
    class Columns:
        """Columns mapping.
        """
        def __getitem__(self, column: sermod.Column) -> tuple:
            if isinstance(column, sermod.Field):
                return tuple([column])
            raise KeyError('Unknown column')
    return Columns()


class TestParser(TupleParser):
    """Frame parser tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def parser(sources: typing.Mapping[framod.Source, tuple], columns: typing.Mapping[sermod.Column, tuple]) -> Frame:
        """Parser fixture.
        """
        return Frame(sources, Series(sources, columns))
