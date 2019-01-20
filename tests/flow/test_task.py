"""
Flow task unit tests.
"""
# pylint: disable=no-self-use
import pickle

import pytest

from forml.flow import task


@pytest.fixture(scope='function')
def actor():
    @task.actor(train='fit', apply='predict')
    class Actor:
        def __init__(self):
            self._features = None
            self._labels = None

        def fit(self, features, labels):
            """
            """
            self._features = features
            self._labels = labels

        def predict(self, features):
            """
            """
            return self._labels

    return Actor()


class TestActor:
    def test_train(self, actor):
        actor.fit('123', 'abc')
        assert actor.predict('xyz') == 'abc'

    def test_serde(self, actor):
        actor.fit('123', 'abc')
        assert pickle.loads(pickle.dumps(actor)).predict('xyz') == 'abc'
