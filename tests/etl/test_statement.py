"""
Statement tests.
"""
import pytest

from forml.etl.schema import frame

from . import schema


class TestQuery(schema.Queryable):
    """Query unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source(school: frame.Table, student: frame.Table) -> frame.Source:
        """Source fixture.
        """
        return student.join(school, student.school == school.sid)
