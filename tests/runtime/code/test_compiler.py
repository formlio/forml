"""
ForML compiler unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml.flow import task
from forml.flow.graph import node, view
from forml.runtime.asset import access
from forml.runtime.code import compiler


@pytest.fixture(scope='session')
def node1(spec: task.Spec) -> node.Worker:
    """Node fixture.
    """
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def node2(spec: task.Spec) -> node.Worker:
    """Node fixture.
    """
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def node3(spec: task.Spec) -> node.Worker:
    """Node fixture.
    """
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def path(node1: node.Worker, node2: node.Worker, node3: node.Worker):
    """Path fixture.
    """
    node2[0].subscribe(node1[0])
    node3[0].subscribe(node2[0])
    return view.Path(node1)


def test_generate(path: view.Path, valid_assets: access.Assets, node1: node.Worker, node2: node.Worker,
                  node3: node.Worker):
    """Compiler generate test.
    """
    with valid_assets.state((node1.gid, node2.gid, node3.gid)) as state:
        compiler.generate(path, state)
