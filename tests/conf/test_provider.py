"""
ForML config unit tests.
"""
# pylint: disable=no-self-use
import abc
import types
import typing

import pytest

from forml.conf import provider as provcfg


class Section(metaclass=abc.ABCMeta):
    """Section test base class.
    """
    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Section]:
        """Provider type.
        """

    def test_default(self, conf: types.ModuleType, provider: typing.Type[provcfg.Section]):
        """Default provider config test.
        """
        assert provider.parse(conf.get(provider.SUBJECT, provider.SELECTOR)) == provider.default


class TestRegistry(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Registry]:
        """Provider type.
        """
        return provcfg.Registry


class TestEngine(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Engine]:
        """Provider type.
        """
        return provcfg.Engine


class TestRunner(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Runner]:
        """Provider type.
        """
        return provcfg.Runner
