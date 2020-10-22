# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Provider tests.
"""
# pylint: disable=no-self-use
import abc
import typing

import pytest

from forml import provider as provmod, error


@pytest.fixture(scope='session')
def alias() -> str:
    """Provider key.
    """
    return 'foobar'


@pytest.fixture(scope='session')
def params() -> typing.Mapping[str, typing.Any]:
    """Default provider kwargs fixture.
    """
    return {'foo': 'bar', 'baz': 10}


@pytest.fixture(scope='session')
def default(alias: str,
            params: typing.Mapping[str, typing.Any]) -> typing.Tuple[str, typing.Mapping[str, typing.Any]]:
    """Default provider spec fixture.
    """
    return alias, params


@pytest.fixture(scope='session')
def interface(default: typing.Tuple[str, typing.Mapping[  # pylint: disable=unused-argument
              str, typing.Any]]) -> typing.Type[provmod.Interface]:
    """Provider fixture.
    """
    _T = typing.TypeVar('_T')

    class Provider(provmod.Interface, typing.Generic[_T], default=default):
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


@pytest.fixture(scope='session')
def provider(interface: typing.Type[provmod.Interface],
             alias: str) -> typing.Type[provmod.Interface]:  # pylint: disable=unused-argument
    """Provider fixture.
    """

    class SubProvider(interface[set], alias=alias):
        """Provider implementation.
        """

        def provide(self) -> None:
            """This provider must not be abstract.
            """

    return SubProvider


def test_isabstract(interface: typing.Type[provmod.Interface], provider: typing.Type[provmod.Interface]):
    """Isabstract inspection unit test.
    """
    assert provmod.isabstract(interface)
    assert not provmod.isabstract(provider)


class TestInterface:
    """Provider interface tests.
    """
    def test_get(self, interface: typing.Type[provmod.Interface], provider: typing.Type[provmod.Interface],
                 alias: str, params: typing.Mapping[str, typing.Any]):
        """Test the provider lookup.
        """
        assert interface[alias] is provider
        assert provider[alias] is provider
        assert interface(val=100) == provider(val=100, **params)
        with pytest.raises(error.Missing):
            assert provider['miss']

    def test_collision(self, provider: typing.Type[provmod.Interface],
                       alias: str):  # pylint: disable=unused-argument
        """Test a colliding provider key.
        """
        with pytest.raises(error.Unexpected):
            class Colliding(provider, alias=alias):
                """colliding implementation.
                """
            assert Colliding


class TestProvider:
    """Testing provider implementation.
    """
    def test_path(self):
        """Test the search path based loading.
        """
        from . import service  # pylint: disable=import-outside-toplevel
        dummy = service.Provider['dummy']
        assert issubclass(dummy, service.Provider)
        assert service.Provider[f'{dummy.__module__}:{dummy.__qualname__}']().serve() == 'dummy'
