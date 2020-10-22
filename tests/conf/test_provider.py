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
ForML config unit tests.
"""
# pylint: disable=no-self-use
import abc
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

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default provider name fixture.
        """

    def test_default(self, provider: typing.Type[provcfg.Section], default: str):
        """Default provider config test.
        """
        assert provider.default.reference == default

    def test_path(self, provider: typing.Type[provcfg.Section]):
        """Path getter test.
        """
        assert isinstance(provider.path, (tuple, list))


class TestRunner(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Runner]:
        """Provider type.
        """
        return provcfg.Runner

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values.
        """
        return 'dask'


class TestRegistry(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Registry]:
        """Provider type.
        """
        return provcfg.Registry

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values.
        """
        return 'filesystem'


class TestFeed(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Feed]:
        """Provider type.
        """
        return provcfg.Feed

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> typing.Any:
        """Provider type.
        """
        return 'presto'

    def test_default(self, provider: typing.Type[provcfg.Section], default: str):
        """Default provider config test.
        """
        assert default in {p.reference for p in provider.default}


class TestSink(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Sink]:
        """Provider type.
        """
        return provcfg.Sink

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values.
        """
        return 'stdout'


class TestSinkMode:
    """Sink modes unit tests.
    """
    def test_default(self):
        """Default modes parsing.
        """
        mode = provcfg.Sink.Mode.default
        assert mode.train.reference == 'stdout'
        assert mode.apply.reference == 'foo'
        assert mode.eval.reference == 'stdout'

    def test_explicit(self):
        """Explicit modes parsing.
        """
        mode = provcfg.Sink.Mode.resolve('foo')
        # pylint: disable=no-member
        assert mode.train.reference == 'foo'
        assert mode.apply.reference == 'foo'
        assert mode.eval.reference == 'foo'
