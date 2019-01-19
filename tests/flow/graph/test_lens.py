"""
Graph unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.flow.graph import node, port, lens


class TestPath:
    """Generic path tests.
    """
    def test_invalid(self, simple: node.Worker, multi: node.Worker):
        """Testing invalid Compound nodes.
        """
        with pytest.raises(AssertionError):  # multi-node not condensable
            lens.Path(multi)
        simple[0].subscribe(multi[0])
        multi[0].subscribe(simple[0])
        with pytest.raises(AssertionError):  # cyclic flow
            lens.Path(multi)


class Path:
    """Path tests.
    """
    def test_type(self, simple: node.Worker, path: lens.Path, btype: typing.Type[lens.Path]):
        assert isinstance(path, btype)
        assert path._head is simple

    def test_copy(self, path: lens.Path):
        """Testing copying path nodes.
        """
        assert isinstance(path.copy(), lens.Path)
        node3 = node.Worker(node.Info('node3', 1), 1, 1)
        node3.train(path._head[0], path._head[0])  # not on path should be ignored
        path.copy()


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
        return lens.Path(simple)

    @staticmethod
    @pytest.fixture(scope='session')
    def btype():
        """Channel path type fixture.
        """
        return lens.Channel


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
        node3 = node.Worker(node.Info('node3', 1), 1, 1)
        node1[0].subscribe(simple[0])
        node2[0].subscribe(node1[0])
        node2[1].subscribe(node1[1])
        node2[0].publish(node3, port.Train())
        return lens.Path(simple)

    @staticmethod
    @pytest.fixture(scope='session')
    def btype():
        """Closure path type fixture.
        """
        return lens.Closure

    def test_publish(self, path: lens.Closure, multi: node.Worker):
        """Testing closure path publishing.
        """
        with pytest.raises(AssertionError):  # closure path publishing
            multi[0].subscribe(path.publisher)
        path.publisher.publish(multi, port.Train())
