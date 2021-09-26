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
from forml.io.dsl._struct import frame, kind, series


class TestOrdering:
    """Ordering unit tests."""

    def test_ordering(self, student: frame.Table):
        """Ordering setup tests."""
        assert (
            series.Ordering(student.score)
            == series.Ordering(student.score, series.Ordering.Direction.ASCENDING)
            == series.Ordering.Direction.ASCENDING(student.score)
            == tuple(series.Ordering.make([student.score]))[0]
            == tuple(series.Ordering.make([student.score, 'ascending']))[0]
            == (student.score, series.Ordering.Direction.ASCENDING)
        )
        assert (
            tuple(series.Ordering.make([student.score, 'asc', student.surname, 'DESC']))
            == tuple(series.Ordering.make([student.score, (student.surname, 'DESCENDING')]))
            == tuple(
                series.Ordering.make(
                    [student.score, series.Ordering(student.surname, series.Ordering.Direction.DESCENDING)]
                )
            )
            == (
                (student.score, series.Ordering.Direction.ASCENDING),
                (student.surname, series.Ordering.Direction.DESCENDING),
            )
        )


class Feature(metaclass=abc.ABCMeta):
    """Base class for feature tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def feature() -> series.Feature:
        """Feature undertest."""

    @abc.abstractmethod
    def test_operable(self, feature: series.Feature):
        """Test the element getter."""

    def test_dissect(self, feature: series.Feature):
        """Test the feature dissection."""
        assert feature in feature.dissect(feature)

    def test_identity(self, feature: series.Feature, school: frame.Table):
        """Test the identity (hashability, equality, sorting)."""
        assert len({feature, feature, school.sid}) == 2
        assert sorted((feature, school.sid)) == sorted((feature, school.sid), key=lambda c: repr(c.operable))

    def test_serilizable(self, feature: series.Feature):
        """Test source serializability."""
        assert cloudpickle.loads(cloudpickle.dumps(feature)) == feature

    def test_kind(self, feature: series.Feature):
        """Test the feature kind."""
        assert isinstance(feature.kind, kind.Any)


class Operable(Feature, metaclass=abc.ABCMeta):
    """Base class for element features."""

    def test_operable(self, feature: series.Operable):
        assert feature.operable == feature

    def test_alias(self, feature: series.Operable):
        """Feature aliasing test."""
        aliased = feature.alias('foo')
        assert aliased.name == 'foo'
        assert aliased.kind == feature.kind


class TestAliased(Feature):
    """Aliased feature tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(series.Literal('baz'),))
    def feature(request) -> series.Aliased:
        """Aliased fixture."""
        return request.param.alias('foobar')

    def test_operable(self, feature: series.Aliased):
        assert isinstance(feature.operable, series.Operable)


class TestLiteral(Operable):
    """Literal feature tests."""

    @staticmethod
    @pytest.fixture(
        scope='session',
        params=(
            True,
            1,
            1.1,
            'foo',
            decimal.Decimal('1.1'),
            datetime.datetime(2020, 5, 5, 5),
            datetime.date(2020, 5, 5),
        ),
    )
    def feature(request) -> series.Literal:
        """Literal fixture."""
        return series.Literal(request.param)


class TestElement(Operable):
    """Element unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def feature(student: frame.Table) -> series.Element:
        """Element fixture."""
        return student.reference().surname


class TestColumn(TestElement):
    """Field unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def feature(student: frame.Table) -> series.Column:
        """Column fixture."""
        return student.surname

    def test_table(self, feature: series.Element, student: frame.Table):
        """Test the feature table reference."""
        assert feature.origin == student


class Predicate(Operable, metaclass=abc.ABCMeta):
    """Predicate features tests."""

    @pytest.mark.parametrize(
        'operation, operator',
        [(lambda a, b: a & b, series.And), (lambda a, b: a | b, series.Or), (lambda a, _: ~series.cast(a), series.Not)],
    )
    def test_logical(
        self,
        operation: typing.Callable[[typing.Any, typing.Any], series.Logical],
        operator: type[series.Predicate],
        feature: series.Predicate,
    ):
        """Logical operators tests."""
        assert isinstance(operation(feature, True), operator)
        with pytest.raises(error.Syntax):
            operation(1, feature)
        if isinstance(operation, series.Bivariate):
            assert isinstance(operation(False, feature), operator)
            assert isinstance(operation(feature, feature), operator)
            with pytest.raises(error.Syntax):
                operation(feature, 1)

    def test_predicate(self, feature: series.Predicate, student: frame.Table):
        """Test predicate features."""
        factors = feature.factors
        assert factors & factors | factors == factors
        for table, expression in factors.items():
            series.Predicate.ensure_is(expression)
            assert len({f.origin for f in series.Column.dissect(expression)}) == 1
            assert table == student


class TestLogical(Predicate):
    """Logical Operations tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(series.And, series.Or, series.Not))
    def feature(request, student: frame.Table) -> series.Logical:
        """Logical fixture."""
        if issubclass(request.param, series.Bivariate):
            return request.param(student.score > 1, student.surname != 'foo')
        return request.param(student.level > 1)


class TestComparison(Predicate):
    """Comparison operations tests."""

    @staticmethod
    @pytest.fixture(
        scope='session',
        params=(
            series.LessThan,
            series.LessEqual,
            series.GreaterThan,
            series.GreaterEqual,
            series.Equal,
            series.NotEqual,
            series.IsNull,
            series.NotNull,
        ),
    )
    def feature(request, student: frame.Table) -> series.Comparison:
        """Comparison fixture."""
        if issubclass(request.param, series.Bivariate):
            return request.param(student.score, 1)
        return request.param(student.surname)

    @pytest.mark.parametrize(
        'operation, operator',
        [
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
        ],
    )
    def test_comparison(
        self,
        operation: typing.Callable[[typing.Any, typing.Any], series.Logical],
        operator: type[series.Logical],
        student: frame.Table,
    ):
        """Comparison operators tests."""
        assert isinstance(operation(student.score, 1).operable, operator)


class TestArithmetic(Operable):
    """Arithmetic operations tests."""

    @staticmethod
    @pytest.fixture(
        scope='session',
        params=(series.Addition, series.Subtraction, series.Division, series.Multiplication, series.Modulus),
    )
    def feature(request, student: frame.Table) -> series.Arithmetic:
        """Arithmetic fixture."""
        return request.param(student.score, 1)

    @pytest.mark.parametrize(
        'operation, operator',
        [
            (lambda a, b: a + b, series.Addition),
            (lambda a, b: a - b, series.Subtraction),
            (lambda a, b: a / b, series.Division),
            (lambda a, b: a * b, series.Multiplication),
            (lambda a, b: a % b, series.Modulus),
        ],
    )
    def test_arithmetic(
        self,
        operation: typing.Callable[[typing.Any, typing.Any], series.Arithmetic],
        operator: type[series.Arithmetic],
        feature: series.Operable,
    ):
        """Arithmetic operators tests."""
        assert isinstance(operation(feature, 1), operator)
        assert isinstance(operation(1, feature), operator)
        assert isinstance(operation(feature, feature), operator)
        with pytest.raises(error.Syntax):
            operation(feature, 'foo')
        with pytest.raises(error.Syntax):
            operation(True, feature)
