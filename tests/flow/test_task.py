"""
Flow task unit tests.
"""
# pylint: disable=no-self-use
import pickle
import typing

import pytest

from forml.flow import task


class TestActor:
    """Actor unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def instance(actor):
        """Instance fixture.
        """
        return actor()

    def test_train(self, instance: task.Actor):
        """Test actor training.
        """
        instance.train('123', 'abc')
        assert instance.predict('xyz') == 'abc'

    def test_serializable(self, instance: task.Actor):
        """Test actor serializability.
        """
        instance.train('123', 'abc')
        assert pickle.loads(pickle.dumps(instance)).predict('xyz') == 'abc'


class TestSpec:
    """Task spec unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def spec(actor: typing.Type[task.Actor]):
        """Task spec fixture.
        """
        return task.Spec(actor, a=1, b=2)

    def test_hashable(self, spec: task.Spec):
        """Test spec hashability.
        """
        assert spec in {spec}

    def test_serializable(self, spec: task.Spec, actor: typing.Type[task.Actor]):
        """Test spec serializability.
        """
        assert pickle.loads(pickle.dumps(spec)).actor == actor
