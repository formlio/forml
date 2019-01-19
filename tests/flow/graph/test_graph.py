"""
Graph unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.flow import graph
from forml.flow.graph import node, port


class TestPath:
    """Generic path tests.
    """
    def test_invalid(self, simple: node.Worker, multi: node.Worker):
        """Testing invalid Compound nodes.
        """
        with pytest.raises(AssertionError):  # multi-node not condensable
            graph.Path(multi)
        simple[0].subscribe(multi[0])
        multi[0].subscribe(simple[0])
        with pytest.raises(AssertionError):  # cyclic flow
            graph.Path(multi)


class Path:
    """Path tests.
    """
    def test_type(self, simple: node.Worker, path: graph.Path, btype: typing.Type[graph.Path]):
        assert isinstance(path, btype)
        assert path.head is simple


class TestChannel(Path):
    """Channel path unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def path(simple: node.Worker):
        """Channel path fixture.
        """
        node1 = node.Worker(node.Info('node1', 1), 1, 2)
        node2 = node.Worker(node.Info('node2', 1), 2, 1)
        node1[0].subscribe(simple[0])
        node2[0].subscribe(node1[0])
        node2[1].subscribe(node1[1])
        return graph.Path(simple)

    @staticmethod
    @pytest.fixture(scope='session')
    def btype():
        """Channel path type fixture.
        """
        return graph.Channel

    def test_copy(self, path: graph.Path):
        """Testing copying path nodes.
        """
        assert isinstance(path.copy(), graph.Path)
        node3 = node.Worker(node.Info('node3', 1), 1, 1)
        node3.train(path.head[0], path.head[0])
        with pytest.raises(AssertionError):  # trained node copy
            path.copy()


class TestClosure(Path):
    """Closure path unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def path(simple: node.Worker):
        """Closure path fixture.
        """
        node1 = node.Worker(node.Info('node1', 1), 1, 2)
        node2 = node.Worker(node.Info('node2', 1), 2, 1)
        node1[0].subscribe(simple[0])
        node2[0].subscribe(node1[0])
        node2[1].subscribe(node1[1])
        node2[0].publish(simple, port.Train())
        return graph.Path(simple)

    @staticmethod
    @pytest.fixture(scope='session')
    def btype():
        """Closure path type fixture.
        """
        return graph.Closure

    def test_copy(self, path: graph.Path):
        """Testing copying path nodes.
        """
        with pytest.raises(AssertionError):
            path.copy()

    def test_publish(self, path: graph.Closure, multi: node.Worker):
        """Testing closure path publishing.
        """
        with pytest.raises(AssertionError):  # closure path publishing
            multi[0].subscribe(path.publisher)
