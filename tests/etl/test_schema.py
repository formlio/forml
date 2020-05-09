"""
ETL unit tests.
"""
# pylint: disable=no-self-use
import abc
import datetime
import decimal

import pytest

from forml.etl import schema


class TestTable:
    """Table unit tests.
    """
    def test_fields(self, table: schema.Table):
        """Fields getter tests.
        """
        assert table.field1.name == 'field1'
        assert table.field2.name == 'baz'
        with pytest.raises(AttributeError):
            _ = table.xyz


class Column(metaclass=abc.ABCMeta):
    """Base class for column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def column() -> schema.Column:
        """Column undertest.
        """

    def test_alias(self, column: schema.Column):
        """Field aliasing test.
        """
        aliased = column.alias('foo')
        assert aliased.name == 'foo'
        assert aliased.kind == column.kind

    def test_logical(self, column: schema.Column):
        """Logical operators tests.
        """
        assert isinstance(column > 1, schema.GreaterThan)
        assert isinstance(column >= 1, schema.GreaterEqual)
        assert isinstance(column < 1, schema.LessThan)
        assert isinstance(column <= 1, schema.LessEqual)
        assert isinstance(column == 1, schema.Equal)
        assert isinstance(column != 1, schema.NotEqual)
        assert isinstance(column & True, schema.And)
        assert isinstance(column | True, schema.Or)
        assert isinstance(~column, schema.Not)

    def test_arithmetic(self, column: schema.Column):
        """Arithmetic operators tests.
        """
        assert isinstance(column + 1, schema.Addition)
        assert isinstance(column - 1, schema.Subtraction)
        assert isinstance(column / 1, schema.Division)
        assert isinstance(column * 1, schema.Multiplication)
        assert isinstance(column % 1, schema.Modulus)


class TestLiteral(Column):
    """Literal column tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(True, 1, 1.1, 'foo', decimal.Decimal('1.1'),
                                             datetime.datetime(2020, 5, 5, 5), datetime.date(2020, 5, 5)))
    def column(request) -> schema.Literal:
        """Literal fixture.
        """
        return schema.Literal(request.param)


class TestField(Column):
    """Field unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def column(table: schema.Table) -> schema.Field:
        """Field fixture.
        """
        return table.field1
