"""
Flow unit tests fixtures.
"""

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


@pytest.fixture(scope='session')
def actor():
    """Actor fixture.
    """
    return task.actor(Actor, apply='predict')
