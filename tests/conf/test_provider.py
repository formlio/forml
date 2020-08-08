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
        key = conf.get(provider.SUBJECT, provider.SELECTOR)
        name = conf.get(conf.OPT_PROVIDER, f'{provider.SUBJECT.upper()}:{key}')
        assert provider.parse(key).name == name


class TestRegistry(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Registry]:
        """Provider type.
        """
        return provcfg.Registry


class TestFeed(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Feed]:
        """Provider type.
        """
        return provcfg.Feed


class TestRunner(Section):
    """Conf unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def provider() -> typing.Type[provcfg.Runner]:
        """Provider type.
        """
        return provcfg.Runner
