"""
ETL unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml.etl.schema import frame


class TestTable:
    """Table unit tests.
    """
    def test_fields(self, student: frame.Table):
        """Fields getter tests.
        """
        assert student.dob.name == 'birthday'
        assert student.score.name == 'score'
        with pytest.raises(AttributeError):
            _ = student.xyz

    def test_select(self, student: frame.Table):
        """Select test.
        """
        query = student.select(student.score)
        assert len(query.columns) == 1
        assert query.columns[0].name == 'score'
