"""
Statement tests.
"""
import pytest
from forml.etl.dsl import statement

from forml.etl.dsl.schema import frame

from . import schema


class TestQuery(schema.Queryable):
    """Query unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Source:
        """Source fixture.
        """
        return statement.Query(student)
