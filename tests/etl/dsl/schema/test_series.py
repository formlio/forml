"""
ETL unit tests.
"""
# pylint: disable=no-self-use
import abc
import datetime
import decimal

import pytest

from forml.etl.dsl.schema import series, frame


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

    def test_identity(self, column: series.Column):
        """Test the identity (hashability + equality).
        """
        assert len({column, column}) == 1

    def test_alias(self, column: series.Column):
        """Field aliasing test.
        """
        aliased = column.alias('foo')
        assert aliased.name == 'foo'
        assert aliased.kind == column.kind

    def test_logical(self, column: series.Column):
        """Logical operators tests.
        """
        assert isinstance(column > 1, series.GreaterThan)
        assert isinstance(column >= 1, series.GreaterEqual)
        assert isinstance(column < 1, series.LessThan)
        assert isinstance(column <= 1, series.LessEqual)
        assert isinstance(column == 1, series.Equal)
        assert isinstance(column != 1, series.NotEqual)
        assert isinstance(column & True, series.And)
        assert isinstance(column | True, series.Or)
        assert isinstance(~column, series.Not)

    def test_arithmetic(self, column: series.Column):
        """Arithmetic operators tests.
        """
        assert isinstance(column + 1, series.Addition)
        assert isinstance(column - 1, series.Subtraction)
        assert isinstance(column / 1, series.Division)
        assert isinstance(column * 1, series.Multiplication)
        assert isinstance(column % 1, series.Modulus)


class TestAliased(Column):
    """Aliased column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(series.Literal('baz'), ))
    def column(request) -> series.Aliased:
        """Aliased fixture.
        """
        return request.param.alias('foobar')

    def test_element(self, column: series.Aliased):
        assert isinstance(column.element, series.Element)


class Element(Column, metaclass=abc.ABCMeta):
    """Base class for element columns.
    """
    def test_element(self, column: series.Element):
        assert column.element == column


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
        assert column.table == student
