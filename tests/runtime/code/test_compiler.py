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
def path(spec: task.Spec):
    """Path fixture.
    """
    node1 = node.Worker(spec, 1, 1)
    node2 = node.Worker(spec, 1, 1)
    node3 = node.Worker(spec, 1, 1)
    node2[0].subscribe(node1[0])
    node3[0].subscribe(node2[0])
    return view.Path(node1)


def test_generate(path: view.Path, valid_assets: access.Assets):
    """Compiler generate test.
    """
    compiler.generate(path, valid_assets.state())
