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
        assert [getattr(c, 'name') for c in
                student.select(student.score, student.surname).columns] == ['score', 'surname']
