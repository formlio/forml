"""
Provider tests.
"""
# pylint: disable=no-self-use
import abc
import typing

import pytest

from forml import provider as provmod


class TestInterface:
    """Provider interface tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def params() -> typing.Mapping[str, typing.Any]:
        """Default provider kwargs fixture.
        """
        return {'foo': 'bar', 'baz': 10}

    @staticmethod
    @pytest.fixture(scope='session')
    def default(subkey: str,
                params: typing.Mapping[str, typing.Any]) -> typing.Tuple[str, typing.Mapping[str, typing.Any]]:
        """Default provider spec fixture.
        """
        return subkey, params

    @staticmethod
    @pytest.fixture(scope='session')
    def provider(default: typing.Tuple[str, typing.Mapping[  # pylint: disable=unused-argument
            str, typing.Any]]) -> typing.Type[provmod.Interface]:
        """Provider fixture.
        """
        class Provider(provmod.Interface, default=default):
            """Provider implementation.
            """
            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def __eq__(self, other):
                return isinstance(other, self.__class__) and other.kwargs == self.kwargs

            @abc.abstractmethod
            def provide(self) -> None:
                """Method required to make provider abstract.
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
            def provide(self) -> None:
                """This provider must not be abstract.
                """

        return SubProvider

    def test_get(self, provider: typing.Type[provmod.Interface], subprovider: typing.Type[provmod.Interface],
                 subkey: str, params: typing.Mapping[str, typing.Any]):
        """Test the provider lookup.
        """
        assert provider[subkey] is subprovider
        assert subprovider[subkey] is subprovider
        assert provider(val=100) == subprovider(val=100, **params)
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
        from tests.provider import service  # pylint: disable=import-outside-toplevel
        assert issubclass(service.Provider['dummy'], service.Provider)
