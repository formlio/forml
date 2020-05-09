"""
ETL unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml.etl.schema import frame
from . import Queryable


class TestTable(Queryable):
    """Table unit tests.
    """
    def test_fields(self, student: frame.Table):
        """Fields getter tests.
        """
        assert student.dob.name == 'birthday'
        assert student.score.name == 'score'
        with pytest.raises(AttributeError):
            _ = student.xyz

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Queryable:
        return student
