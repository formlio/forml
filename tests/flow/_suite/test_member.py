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
Flow segment unit tests.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow
from forml.flow._suite import member


class Composable:
    """Composable tests base class."""

    def test_track(self, composable: flow.Composable):
        """Testing composable track."""
        assert isinstance(composable, flow.Composable)
        assert isinstance(composable.expand(), flow.Trunk)

    def test_noncomposable(self, composable: flow.Composable):
        """Testing noncomposable composition."""
        with pytest.raises(ValueError):
            _ = composable >> 1

    def test_self(self, composable: flow.Composable):
        """Testing self composition."""
        with pytest.raises(flow.TopologyError):
            _ = composable >> composable

    def test_nonlinear(self, composable: flow.Composable, operator: flow.Operator):
        """Testing nonlinear composition."""
        expression = composable >> operator
        with pytest.raises(flow.TopologyError):
            _ = expression >> operator


class TestOrigin(Composable):
    """Origin composable unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Origin composable fixture."""
        return flow.Origin()


class TestCompound(Composable):
    """Compound composable unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Compound composable fixture."""
        return member.Compound(flow.Origin(), flow.Origin())

    def test_compound(self, composable: flow.Composable, operator: flow.Operator):
        """Testing linking action."""
        expression = composable >> operator
        assert isinstance(expression, member.Compound)
