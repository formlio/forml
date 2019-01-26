"""
Flow unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml.flow import task


class Actor:
    """Actor to-be mockup.
    """
    def __init__(self):
        self._features = None
        self._labels = None

    def train(self, features, labels):
        """Train to-be handler.
        """
        self._features = features
        self._labels = labels

    def predict(self, _):
        """Apply to-be handler.
        """
        return self._labels

    def get_params(self):
        """Get hyper-parameters of this actor.
        """
        return {}

    def set_params(self, _):
        """Set hyper-parameters of this actor.
        """


@pytest.fixture(scope='session')
def actor():
    """Actor fixture.
    """
    return task.Wrapped.actor(Actor, apply='predict')
