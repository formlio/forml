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
ETL unit tests.
"""
# pylint: disable=no-self-use
import abc
import datetime
import decimal

import pytest

from forml.io.dsl.schema import series, frame, kind


class Column(metaclass=abc.ABCMeta):
    """Base class for column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def column() -> series.Column:
        """Column undertest.
        """

    @abc.abstractmethod
    def test_element(self, column: series.Column):
        """Test the element getter.
        """

    def test_dissect(self, column: series.Column):
        """Test the column dissection.
        """
        assert column in type(column).dissect(column)

    def test_identity(self, column: series.Column):
        """Test the identity (hashability + equality).
        """
        assert len({column, column}) == 1

    def test_kind(self, column: series.Column):
        """Test the column kind.
        """
        assert isinstance(column.kind, kind.Any)


class Element(Column, metaclass=abc.ABCMeta):
    """Base class for element columns.
    """
    def test_element(self, column: series.Element):
        assert column.element == column

    def test_alias(self, column: series.Element):
        """Field aliasing test.
        """
        aliased = column.alias('foo')
        assert aliased.name == 'foo'
        assert aliased.kind == column.kind

    def test_logical(self, column: series.Element):
        """Logical operators tests.
        """
        # pylint: disable=misplaced-comparison-constant
        assert isinstance(1 < column, series.GreaterThan)
        assert isinstance(column > 1, series.GreaterThan)
        assert isinstance(1 <= column, series.GreaterEqual)
        assert isinstance(column >= 1, series.GreaterEqual)
        assert isinstance(1 > column, series.LessThan)
        assert isinstance(column < 1, series.LessThan)
        assert isinstance(1 >= column, series.LessEqual)
        assert isinstance(column <= 1, series.LessEqual)
        assert isinstance(1 == column, series.Equal)
        assert isinstance(column == 1, series.Equal)
        assert isinstance(1 != column, series.NotEqual)
        assert isinstance(column != 1, series.NotEqual)
        assert isinstance(True & column, series.And)
        assert isinstance(column & True, series.And)
        assert isinstance(True | column, series.Or)
        assert isinstance(column | True, series.Or)
        assert isinstance(~column, series.Not)

    def test_arithmetic(self, column: series.Element):
        """Arithmetic operators tests.
        """
        assert isinstance(1 + column, series.Addition)
        assert isinstance(column + 1, series.Addition)
        assert isinstance(1 - column, series.Subtraction)
        assert isinstance(column - 1, series.Subtraction)
        assert isinstance(1 / column, series.Division)
        assert isinstance(column / 1, series.Division)
        assert isinstance(1 * column, series.Multiplication)
        assert isinstance(column * 1, series.Multiplication)
        assert isinstance(1 % column, series.Modulus)
        assert isinstance(column % 1, series.Modulus)


class TestAliased(Column):
    """Aliased column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.Literal('baz'),))
    def column(request) -> series.Aliased:
        """Aliased fixture.
        """
        return request.param.alias('foobar')

    def test_element(self, column: series.Aliased):
        assert isinstance(column.element, series.Element)


class TestLiteral(Element):
    """Literal column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(True, 1, 1.1, 'foo', decimal.Decimal('1.1'),
                                             datetime.datetime(2020, 5, 5, 5), datetime.date(2020, 5, 5)))
    def column(request) -> series.Literal:
        """Literal fixture.
        """
        return series.Literal(request.param)


class TestField(Element):
    """Field unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def column(student: frame.Table) -> series.Field:
        """Field fixture.
        """
        return student.surname

    def test_table(self, column: series.Field, student: frame.Table):
        """Test the column table reference.
        """
        assert column.source == student
