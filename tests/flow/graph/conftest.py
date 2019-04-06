"""
Graph unit tests fixtures.
"""

import pytest

from forml.flow import task
from forml.flow.graph import node as grnode


@pytest.fixture(scope='function')
def simple(spec: task.Spec) -> grnode.Worker:
    """Simple node fixture with 1 input and 1 output apply port.
    """
    return grnode.Worker(spec, 1, 1)


@pytest.fixture(scope='function')
def multi(spec: task.Spec) -> grnode.Worker:
    """Multi port node fixture (2 input and 2 output apply port).
    """
    return grnode.Worker(spec, 2, 2)
