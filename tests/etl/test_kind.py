"""
ETL kinds unit tests.
"""
# pylint: disable=no-self-use
import abc
import typing

import pytest

from forml.etl import kind as kindmod


class Data(metaclass=abc.ABCMeta):
    """Common test base class.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def kind() -> typing.Type[kindmod.Data]:
        """Undertest kind type.
        """

    def test_singleton(self, kind: typing.Type[kindmod.Data]):
        """Test the instances are singletons.
        """
        assert kind() is kind()


class TestBoolean(Data):
    """Boolean type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Boolean
