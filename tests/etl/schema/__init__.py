"""
Schema test commons.
"""
# pylint: disable=no-self-use
import abc

import pytest

from forml.etl.schema import frame


class Queryable(metaclass=abc.ABCMeta):
    """Base class for queryable tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def source() -> frame.Queryable:
        """Undertest source.
        """

    def test_select(self, source: frame.Table, student: frame.Table):
        """Select test.
        """
        assert [getattr(c, 'name') for c in
                source.select(student.score, student.surname).columns] == ['score', 'surname']
