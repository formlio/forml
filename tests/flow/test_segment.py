"""
Flow segment unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow
from forml.flow import segment


class Composable:
    """Composable tests base class.
    """
    def test_track(self, composable: segment.Composable):
        """Testing composable track.
        """
        assert isinstance(composable, segment.Composable)
        assert isinstance(composable.track(), segment.Track)


class TestOrigin(Composable):
    """Origin composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def composable():
        """Origin composable fixture.
        """
        return segment.Origin()


class TestExpression(Composable):
    """Recursive composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def composable(operator: flow.Operator):
        """Expression composable fixture.
        """
        return segment.Expression(operator, segment.Origin())

    def test_expression(self, composable: segment.Composable, operator: flow.Operator):
        """Testing linking action.
        """
        expression = composable >> operator
        assert isinstance(expression, segment.Expression)
