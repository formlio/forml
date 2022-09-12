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
import abc
import typing

import pytest

from forml import setup


class Section(metaclass=abc.ABCMeta):
    """Section test base class."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def provider() -> type[setup.Provider]:
        """Provider type."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default provider name fixture."""

    def test_default(self, provider: type[setup.Provider], default: str):
        """Default provider config test."""
        assert provider.default.reference == default

    def test_path(self, provider: type):
        """Path getter test."""
        assert isinstance(provider.path, (tuple, list))


class TestRunner(Section):
    """Conf unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> type[setup.Runner]:
        """Provider type."""
        return setup.Runner

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values."""
        return 'bar'


class TestRegistry(Section):
    """Conf unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> type[setup.Registry]:
        """Provider type."""
        return setup.Registry

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values."""
        return 'bar'


class TestFeed(Section):
    """Conf unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> type[setup.Feed]:
        """Provider type."""
        return setup.Feed

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> typing.Any:
        """Provider type."""
        return 'bar'

    def test_default(self, provider: type[setup.Provider], default: str):
        """Default provider config test."""
        assert default in {p.reference for p in provider.default}


class TestSink(Section):
    """Conf unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> type[setup.Sink]:
        """Provider type."""
        return setup.Sink

    @staticmethod
    @pytest.fixture(scope='session')
    def default() -> str:
        """Default values."""
        return 'foo'


class TestSinkMode:
    """Sink modes unit tests."""

    def test_default(self):
        """Default modes parsing."""
        mode = setup.Sink.Mode.default
        assert mode.apply.reference == 'bar'
        assert mode.eval.reference == 'baz'

    def test_explicit(self):
        """Explicit modes parsing."""
        mode = setup.Sink.Mode.resolve('foo')
        # pylint: disable=no-member
        assert mode.apply.reference == 'foo'
        assert mode.eval.reference == 'foo'
