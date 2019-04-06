"""
Graph node ports unit tests.
"""
# pylint: disable=no-self-use

import abc

import pytest

from forml.flow.graph import port as pmod


class Type(metaclass=abc.ABCMeta):
    """Base class for port types tests.
    """
    @staticmethod
    @abc.abstractmethod
    def port() -> pmod.Type:
        """Port fixture
        """

    def test_int(self, port: pmod.Type):
        """Testing type of port type.
        """
        assert isinstance(port, int)


class Singleton(Type):  # pylint: disable=abstract-method
    """Base class for singleton port.
    """
    def test_singleton(self, port: pmod.Type):
        """Test ports are singletons.
        """
        assert port.__class__() is port.__class__()


class TestTrain(Singleton):
    """Train port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Train:
        """Port type fixture
        """
        return pmod.Train()


class TestLabel(Singleton):
    """Label port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Label:
        """Port type fixture
        """
        return pmod.Label()


class TestApply(Type):
    """Apply port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Type:
        """Port type fixture
        """
        return pmod.Apply(1)
