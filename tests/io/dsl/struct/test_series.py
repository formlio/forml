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
import typing

import cloudpickle
import pytest

from forml.io.dsl import error
from forml.io.dsl.struct import series, frame, kind


class TestOrdering:
    """Ordering unit tests.
    """
    def test_ordering(self, student: frame.Table):
        """Ordering setup tests.
        """
        assert series.Ordering(student.score) == \
               series.Ordering(student.score, series.Ordering.Direction.ASCENDING) == \
               series.Ordering.Direction.ASCENDING(student.score) == \
               tuple(series.Ordering.make([student.score]))[0] == \
               tuple(series.Ordering.make([student.score, 'ascending']))[0] == \
               (student.score, series.Ordering.Direction.ASCENDING)
        assert tuple(series.Ordering.make([student.score, 'asc', student.surname, 'DESC'])) == \
               tuple(series.Ordering.make([student.score, (student.surname, 'DESCENDING')])) == \
               tuple(series.Ordering.make([student.score, series.Ordering(student.surname,
                                                                          series.Ordering.Direction.DESCENDING)])) == \
               ((student.score, series.Ordering.Direction.ASCENDING),
                (student.surname, series.Ordering.Direction.DESCENDING))


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
    def test_operable(self, column: series.Column):
        """Test the element getter.
        """

    def test_dissect(self, column: series.Column):
        """Test the column dissection.
        """
        assert column in column.dissect(column)

    def test_identity(self, column: series.Column, school: frame.Table):
        """Test the identity (hashability, equality, sorting).
        """
        assert len({column, column, school.sid}) == 2
        assert sorted((column, school.sid)) == sorted((column, school.sid), key=lambda c: repr(c.operable))

    def test_serilizable(self, column: series.Column):
        """Test source serializability.
        """
        assert cloudpickle.loads(cloudpickle.dumps(column)) == column

    def test_kind(self, column: series.Column):
        """Test the column kind.
        """
        assert isinstance(column.kind, kind.Any)


class Operable(Column, metaclass=abc.ABCMeta):
    """Base class for element columns.
    """
    def test_operable(self, column: series.Operable):
        assert column.operable == column

    def test_alias(self, column: series.Operable):
        """Field aliasing test.
        """
        aliased = column.alias('foo')
        assert aliased.name == 'foo'
        assert aliased.kind == column.kind


class TestAliased(Column):
    """Aliased column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.Literal('baz'),))
    def column(request) -> series.Aliased:
        """Aliased fixture.
        """
        return request.param.alias('foobar')

    def test_operable(self, column: series.Aliased):
        assert isinstance(column.operable, series.Operable)


class TestLiteral(Operable):
    """Literal column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(True, 1, 1.1, 'foo', decimal.Decimal('1.1'),
                                             datetime.datetime(2020, 5, 5, 5), datetime.date(2020, 5, 5)))
    def column(request) -> series.Literal:
        """Literal fixture.
        """
        return series.Literal(request.param)


class TestElement(Operable):
    """Element unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def column(student: frame.Table) -> series.Element:
        """Element fixture.
        """
        return student.reference().surname


class TestField(TestElement):
    """Field unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def column(student: frame.Table) -> series.Field:
        """Field fixture.
        """
        return student.surname

    def test_table(self, column: series.Element, student: frame.Table):
        """Test the column table reference.
        """
        assert column.origin == student


class Predicate(Operable, metaclass=abc.ABCMeta):
    """Predicate columns tests.
    """
    @pytest.mark.parametrize('operation, operator', [
        (lambda a, b: a & b, series.And),
        (lambda a, b: a | b, series.Or),
        (lambda a, _: ~series.cast(a), series.Not)
    ])
    def test_logical(self, operation: typing.Callable[[typing.Any, typing.Any], series.Logical],
                     operator: typing.Type[series.Predicate], column: series.Predicate):
        """Logical operators tests.
        """
        assert isinstance(operation(column, True), operator)
        with pytest.raises(error.Syntax):
            operation(1, column)
        if isinstance(operation, series.Bivariate):
            assert isinstance(operation(False, column), operator)
            assert isinstance(operation(column, column), operator)
            with pytest.raises(error.Syntax):
                operation(column, 1)

    def test_predicate(self, column: series.Predicate, student: frame.Table):
        """Test predicate features.
        """
        factors = column.factors
        assert factors & factors | factors == factors
        for table, expression in factors.items():
            series.Predicate.ensure_is(expression)
            assert len({f.origin for f in series.Field.dissect(expression)}) == 1
            assert table == student


class TestLogical(Predicate):
    """Logical Operations tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.And, series.Or, series.Not))
    def column(request, student: frame.Table) -> series.Logical:
        """Logical fixture.
        """
        if issubclass(request.param, series.Bivariate):
            return request.param(student.score > 1, student.surname != 'foo')
        return request.param(student.level > 1)


class TestComparison(Predicate):
    """Comparison operations tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.LessThan, series.LessEqual, series.GreaterThan, series.GreaterEqual,
                                             series.Equal, series.NotEqual, series.IsNull, series.NotNull))
    def column(request, student: frame.Table) -> series.Comparison:
        """Comparison fixture.
        """
        if issubclass(request.param, series.Bivariate):
            return request.param(student.score, 1)
        return request.param(student.surname)

    @pytest.mark.parametrize('operation, operator', [
        (lambda c, i: c > i, series.GreaterThan),
        (lambda c, i: i < c, series.GreaterThan),
        (lambda c, i: c >= i, series.GreaterEqual),
        (lambda c, i: i <= c, series.GreaterEqual),
        (lambda c, i: c < i, series.LessThan),
        (lambda c, i: i > c, series.LessThan),
        (lambda c, i: c <= i, series.LessEqual),
        (lambda c, i: i >= c, series.LessEqual),
        (lambda c, i: c == i, series.Equal),
        (lambda c, i: i == c, series.Equal),
        (lambda c, i: c != i, series.NotEqual),
        (lambda c, i: i != c, series.NotEqual),
    ])
    def test_comparison(self, operation: typing.Callable[[typing.Any, typing.Any], series.Logical],
                        operator: typing.Type[series.Logical], student: frame.Table):
        """Comparison operators tests.
        """
        assert isinstance(operation(student.score, 1).operable, operator)


class TestArithmetic(Operable):
    """Arithmetic operations tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.Addition, series.Subtraction, series.Division,
                                             series.Multiplication, series.Modulus))
    def column(request, student: frame.Table) -> series.Arithmetic:
        """Arithmetic fixture.
        """
        return request.param(student.score, 1)

    @pytest.mark.parametrize('operation, operator', [
        (lambda a, b: a + b, series.Addition),
        (lambda a, b: a - b, series.Subtraction),
        (lambda a, b: a / b, series.Division),
        (lambda a, b: a * b, series.Multiplication),
        (lambda a, b: a % b, series.Modulus),
    ])
    def test_arithmetic(self, operation: typing.Callable[[typing.Any, typing.Any], series.Arithmetic],
                        operator: typing.Type[series.Arithmetic], column: series.Operable):
        """Arithmetic operators tests.
        """
        assert isinstance(operation(column, 1), operator)
        assert isinstance(operation(1, column), operator)
        assert isinstance(operation(column, column), operator)
        with pytest.raises(error.Syntax):
            operation(column, 'foo')
        with pytest.raises(error.Syntax):
            operation(True, column)
