"""
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml import etl
from forml.etl import schema, kind


@pytest.fixture(scope='session')
def base() -> schema.Table:
    """Base table fixture.
    """

    class Table(metaclass=schema.Table):  # pylint: disable=invalid-metaclass
        """Base table.
        """
        field1 = etl.Field(kind.Integer())

    return Table


@pytest.fixture(scope='session')
def table(base: schema.Table) -> schema.Table:
    """Extended table fixture.
    """

    class Table(base):
        """Extended table.
        """
        field2 = etl.Field(kind.Float(), 'baz')

    return Table
