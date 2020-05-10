"""
Schema test commons.
"""
# pylint: disable=no-self-use
import abc

import pytest

from forml.etl import function, statement
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

    def test_select(self, source: frame.Queryable, student: frame.Table):
        """Select test.
        """
        assert source.select(student.score).columns[0].name == 'score'
        assert [getattr(c, 'name') for c in
                source.select([student.score, student.surname]).columns] == ['score', 'surname']

    def test_filter(self, source: frame.Queryable, student: frame.Table):
        """Filter test.
        """
        with pytest.raises(ValueError):
            source.filter(student.score + 1)
        assert isinstance(source.filter(student.score > 2).condition.expression, function.GreaterThan)

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
        grouped = source.groupby(student.school)
        assert isinstance(grouped.grouping, statement.Grouping)
        assert grouped.grouping.columns[0].name == 'school'
        assert grouped.grouping.condition is None
        grouped = source.groupby([student.school, student.level], student.score > 2)
        assert isinstance(grouped.grouping, statement.Grouping)
        assert [getattr(c, 'name') for c in grouped.grouping.columns] == ['school', 'level']
        assert isinstance(grouped.grouping.condition.expression, function.GreaterThan)

    def test_orderby(self, source: frame.Queryable, student: frame.Table):
        """Orderby test.
        """
        ordered = source.orderby(student.school)
        assert isinstance(ordered.ordering, statement.Ordering)
        assert ordered.ordering.columns[0].name == 'school'
        assert ordered.ordering.direction == statement.Ordering.Direction.ASCENDING
        ordered = source.orderby([student.school, student.level], statement.Ordering.Direction.DESCENDING)
        assert isinstance(ordered.ordering, statement.Ordering)
        assert [getattr(c, 'name') for c in ordered.ordering.columns] == ['school', 'level']
        assert ordered.ordering.direction == statement.Ordering.Direction.DESCENDING

    def test_limit(self, source: frame.Queryable):
        """Limit test.
        """
        assert source.limit(1).rows == (1, 0)
        assert source.limit(1, 1).rows == (1, 1)
