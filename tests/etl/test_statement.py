"""
Statement tests.
"""
import pytest

from forml.etl import schema


class TestQuery:
    """Query unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source(school: schema.Table, student: schema.Table) -> schema.Source:
        """Source fixture.
        """
