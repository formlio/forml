"""
Graph unit tests fixtures.
"""

import pytest

from forml.flow.graph import node as grnode


@pytest.fixture(scope='function')
def simple():
    """Simple node fixture with 1 input and 1 output apply port.
    """
    return grnode.Worker(grnode.Info('simple', 1), 1, 1)


@pytest.fixture(scope='function')
def multi():
    """Multi port node fixture (2 input and 2 output apply port).
    """
    return grnode.Worker(grnode.Info('multi', 1), 2, 2)
