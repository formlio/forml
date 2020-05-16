"""
Schema test commons.
"""
# pylint: disable=no-self-use
import abc

import pytest

from forml.etl.dsl import statement, function
from forml.etl.dsl.schema import frame


class Queryable(metaclass=abc.ABCMeta):
    """Base class for queryable tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def source() -> frame.Queryable:
        """Undertest source.
        """

    def test_select(self, source: frame.Queryable, student: frame.Table):
        """Select test.
        """
        assert source.select(student.score).columns[0] == student.score
        assert source.select(student.score, student.surname).columns == (student.score, student.surname)

    def test_where(self, source: frame.Queryable, student: frame.Table):
        """Where test.
        """
        with pytest.raises(ValueError):
            source.where(student.score + 1)
        assert isinstance(source.where(student.score > 2).prefilter, function.GreaterThan)

    def test_having(self, source: frame.Queryable, student: frame.Table):
        """Having test.
        """
        with pytest.raises(ValueError):
            source.having(student.score + 1)
        assert isinstance(source.having(student.score > 2).postfilter, function.GreaterThan)

    def test_join(self, source: frame.Queryable, student: frame.Table, school: frame.Table):
        """Join test.
        """
        joined = source.join(school, student.school == school.sid)
        assert isinstance(joined.source, statement.Join)
        assert isinstance(joined.source.right, type(school))
        assert joined.source.kind == statement.Join.Kind.LEFT

    def test_groupby(self, source: frame.Queryable, student: frame.Table):
        """Groupby test.
        """
        assert source.groupby(student.score).grouping[0] == student.score
        assert source.groupby(student.score, student.surname).grouping == (student.score, student.surname)

    def test_orderby(self, source: frame.Queryable, student: frame.Table):
        """Orderby test.
        """
        assert source.orderby(student.score).ordering[0] == (
            student.score, statement.Ordering.Direction.ASCENDING)
        assert source.orderby(statement.Ordering(student.score)).ordering[0] == (
            student.score, statement.Ordering.Direction.ASCENDING)
        assert source.orderby(statement.Ordering(student.score,
                                                 statement.Ordering.Direction.DESCENDING)).ordering[0] == (
                                                     student.score, statement.Ordering.Direction.DESCENDING)
        assert source.orderby(student.score, 'descending').ordering[0] == (
            student.score, statement.Ordering.Direction.DESCENDING)
        assert source.orderby(student.score, statement.Ordering.Direction.DESCENDING).ordering[0] == (
            student.score, statement.Ordering.Direction.DESCENDING)

        assert source.orderby(student.score, student.surname, 'descending').ordering == (
            (student.score, statement.Ordering.Direction.ASCENDING),
            (student.surname, statement.Ordering.Direction.DESCENDING))
        assert source.orderby(student.score, (student.surname, 'descending')).ordering == (
            (student.score, statement.Ordering.Direction.ASCENDING),
            (student.surname, statement.Ordering.Direction.DESCENDING))

    def test_limit(self, source: frame.Queryable):
        """Limit test.
        """
        assert source.limit(1).rows == (1, 0)
        assert source.limit(1, 1).rows == (1, 1)
