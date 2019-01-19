"""
Flow segment unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow
from forml.flow import segment


@pytest.fixture(scope='session')
def operator():
    """Operator fixture.
    """
    class Operator(flow.Operator):
        """Operator mock.
        """
        def compose(self, left: segment.Builder) -> segment.Track:
            """Dummy composition.
            """
            return left.track()

    return Operator()


class Builder:
    """Builder tests base class.
    """
    def test_track(self, builder: segment.Builder):
        """Testing builder track.
        """
        assert isinstance(builder, segment.Builder)
        assert isinstance(builder.track(), segment.Track)


class TestOrigin(Builder):
    """Origin builder unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def builder():
        """Origin builder fixture.
        """
        return segment.Origin()


class TestRecursive(Builder):
    """Recursive builder unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def builder(operator: flow.Operator):
        """Recursive builder fixture.
        """
        return segment.Recursive(operator, segment.Origin())


class TestLink:
    """Segment linking unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def link(operator: flow.Operator):
        """Link fixture.
        """
        return segment.Link(operator, segment.Recursive(operator, segment.Origin()))

    def test_link(self, link: segment.Link, operator: flow.Operator):
        """Testing linking action.
        """
        linked = link >> operator
        assert isinstance(linked, segment.Link)
        assert isinstance(linked.pipeline, flow.Pipeline)
