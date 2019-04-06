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
        assert isinstance(composable.expand(), segment.Track)

    def test_noncomposable(self, composable: segment.Composable):
        """Testing noncomposable composition.
        """
        with pytest.raises(ValueError):
            composable >> 1  # pylint: disable=pointless-statement

    def test_self(self, composable: segment.Composable):
        """Testing self composition.
        """
        with pytest.raises(ArithmeticError):
            composable >> composable  # pylint: disable=pointless-statement

    def test_nonlinear(self, composable: segment.Composable, operator: flow.Operator):
        """Testing nonlinear composition.
        """
        expression = composable >> operator
        with pytest.raises(ArithmeticError):
            expression >> operator  # pylint: disable=pointless-statement


class TestOrigin(Composable):
    """Origin composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Origin composable fixture.
        """
        return segment.Origin()


class TestExpression(Composable):
    """Recursive composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Expression composable fixture.
        """
        return segment.Expression(segment.Origin(), segment.Origin())

    def test_expression(self, composable: segment.Composable, operator: flow.Operator):
        """Testing linking action.
        """
        expression = composable >> operator
        assert isinstance(expression, segment.Expression)
