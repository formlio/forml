"""
Schema test commons.
"""
# pylint: disable=no-self-use
import abc
import typing

import pytest

from forml.etl.dsl import statement, function
from forml.etl.dsl.schema import frame, series


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

    @staticmethod
    def _condition(source: frame.Queryable, student: frame.Table,
                   handler: typing.Callable[[frame.Queryable, series.Expression], statement.Query],
                   target: typing.Callable[[statement.Query], series.Expression]):
        """Common condition testing routine for where/having.
        """
        with pytest.raises(ValueError):
            handler(source, student.score + 1)
        assert target(handler(source, student.score > 2)) == function.GreaterThan(student.score, series.Literal(2))
        assert target(handler(handler(source, student.score > 2), student.level < 4)) == \
               function.And(function.LessThan(student.level, series.Literal(4)),
                            function.GreaterThan(student.score, series.Literal(2)))

    def test_where(self, source: frame.Queryable, student: frame.Table):
        """Where test.
        """
        self._condition(source, student, lambda s, e: s.where(e), lambda q: q.prefilter)

    def test_having(self, source: frame.Queryable, student: frame.Table):
        """Having test.
        """
        self._condition(source, student, lambda s, e: s.having(e), lambda q: q.postfilter)

    def test_join(self, source: frame.Queryable, student: frame.Table, school: frame.Table):
        """Join test.
        """
        joined = source.join(school, student.school == school.sid)
        assert isinstance(joined.source, statement.Join)
        assert joined.source.kind == statement.Join.Kind.LEFT
        assert joined.source.right == school
        assert joined.source.condition == function.Equal(student.school, school.sid)

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
