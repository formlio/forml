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
Graph node ports unit tests.
"""
# pylint: disable=no-self-use

import abc

import pytest

from forml.flow.graph import port as pmod


class Type(metaclass=abc.ABCMeta):
    """Base class for port types tests."""

    @staticmethod
    @abc.abstractmethod
    def port() -> pmod.Type:
        """Port fixture"""

    def test_int(self, port: pmod.Type):
        """Testing type of port type."""
        assert isinstance(port, int)


class Singleton(Type):  # pylint: disable=abstract-method
    """Base class for singleton port."""

    def test_singleton(self, port: pmod.Type):
        """Test ports are singletons."""
        assert port.__class__() is port.__class__()


class TestTrain(Singleton):
    """Train port type tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Train:
        """Port type fixture"""
        return pmod.Train()


class TestLabel(Singleton):
    """Label port type tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Label:
        """Port type fixture"""
        return pmod.Label()


class TestApply(Type):
    """Apply port type tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def port() -> pmod.Type:
        """Port type fixture"""
        return pmod.Apply(1)
