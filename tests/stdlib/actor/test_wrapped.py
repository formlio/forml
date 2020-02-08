"""
Wrapped actor unit tests.
"""
# pylint: disable=no-self-use
import pytest
from forml.flow import task
from forml.stdlib.actor import wrapped


class TestFunction:
    """Wrapped function unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def actor() -> task.Actor:
        """Actor fixture.
        """
        @wrapped.Function.actor
        def replace(string: str, old: str, new: str, count=-1):
            """Actor wrapped function.
            """
            return string.replace(old, new, count)
        return replace

    def test_signature(self, actor: task.Actor):
        """Test the actor signature.
        """
        with pytest.raises(TypeError):
            actor(foo='bar')
        actor(old='asd')

    def test_apply(self, actor: task.Actor):
        """Actor applying test.
        """
        assert actor(old='baz', new='foo').apply('baz bar') == 'foo bar'
