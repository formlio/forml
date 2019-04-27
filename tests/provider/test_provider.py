"""
Provider tests.
"""
# pylint: disable=no-self-use

import typing

import pytest

from forml import provider as provmod


class TestInterface:
    """Provider interface tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provmod.Interface]:
        """Provider fixture.
        """
        class Provider(provmod.Interface):
            """Provider implementation.
            """
        return Provider

    @staticmethod
    @pytest.fixture(scope='session')
    def subkey() -> str:
        """Provider key.
        """
        return 'subkey'

    @staticmethod
    @pytest.fixture(scope='session')
    def subprovider(provider: typing.Type[provmod.Interface],
                    subkey: str) -> typing.Type[provmod.Interface]:  # pylint: disable=unused-argument
        """Provider fixture.
        """
        class SubProvider(provider, key=subkey):
            """Provider implementation.
            """
        return SubProvider

    def test_get(self, provider: typing.Type[provmod.Interface],
                 subprovider: typing.Type[provmod.Interface], subkey: str):
        """Test the provider lookup.
        """
        assert provider[subkey] is subprovider
        assert subprovider[subkey] is subprovider
        with pytest.raises(provmod.Error):
            assert subprovider['miss']

    def test_collision(self, subprovider: typing.Type[provmod.Interface],
                       subkey: str):  # pylint: disable=unused-argument
        """Test a colliding provider key.
        """
        with pytest.raises(provmod.Error):
            class Colliding(subprovider, key=subkey):
                """colliding implementation.
                """
            assert Colliding


class TestProvider:
    """Testing provider implementation.
    """
    def test_staged(self):
        """Test the staged imports loading.
        """
        from tests.provider import service
        assert issubclass(service.Provider['dummy'], service.Provider)
