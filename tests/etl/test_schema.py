"""
ETL unit tests.
"""
# pylint: disable=no-self-use
import pytest

from forml import etl
from forml.etl import schema


@pytest.fixture
def base() -> schema.Table:
    """Base table fixture.
    """

    class Table(metaclass=schema.Table):  # pylint: disable=invalid-metaclass
        """Base table.
        """
        field1 = etl.Field('int')

    return Table


@pytest.fixture
def table(base: schema.Table) -> schema.Table:
    """Extended table fixture.
    """

    class Table(base):
        """Extended table.
        """
        field2 = etl.Field('float', 'baz')

    return Table


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


class TestField:
    """Field unit tests.
    """
    @staticmethod
    @pytest.fixture
    def field1(table: schema.Table) -> schema.Field:
        """Field fixture.
        """
        return table.field1

    def test_alias(self, field1: schema.Field):
        """Field aliasing test.
        """
        assert field1.alias('foo').name == 'foo'
