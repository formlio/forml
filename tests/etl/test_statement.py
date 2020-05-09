"""
Statement tests.
"""
import pytest

from forml.etl.schema import frame


class TestQuery:
    """Query unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source(school: frame.Table, student: frame.Table) -> frame.Source:
        """Source fixture.
        """
