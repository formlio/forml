"""
Graph node unit tests.
"""
# pylint: disable=no-self-use

import abc

import pytest

from forml.flow.graph import node as grnode, port


class Atomic(metaclass=abc.ABCMeta):
    """Base class for node tests.
    """
    @staticmethod
    @abc.abstractmethod
    def node():
        """Node fixture.
        """

    def test_copy(self, node: grnode.Atomic):
        """Test for node copy method.
        """
        assert isinstance(node.copy(), grnode.Atomic)

    def test_publish(self, node: grnode.Atomic, simple: grnode.Worker):
        """Test node publishing.
        """
        node[0].publish(simple, port.Apply(0))
        assert any(simple is s.node and s.port == 0 for s in node.output[0])
        assert port.Apply(0) in simple.input
        with pytest.raises(AssertionError):  # already subscribed
            node[0].publish(simple, port.Apply(0))
        with pytest.raises(AssertionError):  # self subscription
            node[0].publish(node, port.Apply(0))
        with pytest.raises(AssertionError):  # apply-train collision
            node[0].publish(simple, port.Train())

    def test_subscribe(self, node: grnode.Atomic, simple: grnode.Worker):
        """Test node subscribing.
        """
        simple[0].subscribe(node[0])
        assert any(simple is s.node and s.port == 0 for s in node.output[0])
        assert port.Apply(0) in simple.input
        with pytest.raises(AssertionError):  # self subscription
            simple[0].subscribe(node[0])


class TestWorker(Atomic):
    """Specific tests for the worker node.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def node():
        """Node fixture.
        """
        return grnode.Worker(grnode.Info('worker', 1), 1, 1)

    def test_train(self, node: grnode.Worker, simple: grnode.Worker, multi: grnode.Worker):
        """Test train subscription
        """
        node.train(multi[0], multi[1])
        assert any(node is s.node and s.port == port.Train() for s in multi.output[0])
        assert any(node is s.node and s.port == port.Label() for s in multi.output[1])
        assert node.trained
        with pytest.raises(AssertionError):  # train-apply collision
            node[0].subscribe(simple[0])


class TestFuture(Atomic):
    """Specific tests for the future node.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def node():
        """Node fixture.
        """
        return grnode.Future()

    def test_republish(self, node: grnode.Worker, simple: grnode.Worker, multi: grnode.Worker):
        """Test republishing after subscribing future node to a real worker.
        """
        node[0].publish(multi, port.Train())
        node[0].subscribe(simple[0])
        assert any(multi is s.node and s.port == port.Train() for s in simple.output[0])

    def test_invalid(self, node: grnode.Worker, simple: grnode.Worker, multi: grnode.Worker):
        """Testing invalid future subscriptions.
        """
        with pytest.raises(AssertionError):  # no subscriptions
            node[0].subscribe(simple)
        node[0].publish(multi, port.Train())
        with pytest.raises(AssertionError):  # multi-output publisher
            node[0].subscribe(multi)


class TestFactory:
    """Node factory tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def factory():
        """Factory fixture.
        """
        return grnode.Factory('factory', 1, 1)

    def test_instance(self, factory: grnode.Factory):
        """Testing node creation.
        """
        assert factory.node().info.instance == 1
        assert factory.node().info.instance == 1
        factory = grnode.Factory('factory', 1, 1)
        assert factory.node().info.instance == 2
