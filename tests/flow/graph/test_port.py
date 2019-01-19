"""
Graph node ports unit tests.
"""
# pylint: disable=no-self-use

import abc

import pytest

from forml.flow.graph import port


class Type(metaclass=abc.ABCMeta):
    """Base class for port types tests.
    """
    @staticmethod
    @abc.abstractmethod
    def ptype():
        """Port type fixture
        """

    def test_type(self, ptype):
        """Testing type of port type.
        """
        assert isinstance(ptype, port.Type)


class TestTrain(Type):
    """Train port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def ptype():
        """Port type fixture
        """
        return port.Train()


class TestLabel(Type):
    """Label port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def ptype():
        """Port type fixture
        """
        return port.Label()


class TestApply(Type):
    """Apply port type tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def ptype():
        """Port type fixture
        """
        return port.Apply(1)
