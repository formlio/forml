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
import typing

import pytest

from forml.flow import task
from forml.flow.pipeline import topology
from forml.lib.flow.operator.generic import simple


class TestMapper:
    """Simple mapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def operator(actor: typing.Type[task.Actor]):
        """Operator fixture."""
        return simple.Mapper.operator(actor)()  # pylint: disable=no-value-for-parameter

    def test_compose(self, operator: topology.Operator):
        """Operator composition test."""
        operator.compose(topology.Origin())
