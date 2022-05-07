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
Simple operator unit tests.
"""
# pylint: disable=no-self-use
import abc

import pytest

from forml import flow
from forml.pipeline import wrap


class Base(abc.ABC):
    """Simple operator unit tests base class."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def operator(actor_type: type[flow.Actor]) -> flow.Operator:
        """Operator fixture."""

    def test_compose(self, operator: flow.Operator):
        """Operator composition test."""
        operator.compose(flow.Origin())


class TestMapper(Base):
    """Simple mapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def operator(actor_type: type[flow.Actor]):
        """Operator fixture."""
        return wrap.Mapper.operator(actor_type)()  # pylint: disable=no-value-for-parameter


class TestConsumer(Base):
    """Simple consumer unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def operator(actor_type: type[flow.Actor]):
        """Operator fixture."""
        return wrap.Consumer.operator(actor_type)()  # pylint: disable=no-value-for-parameter
