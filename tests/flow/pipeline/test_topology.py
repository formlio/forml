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

from forml.flow import pipeline, error
from forml.flow.pipeline import topology


class Composable:
    """Composable tests base class.
    """
    def test_track(self, composable: topology.Composable):
        """Testing composable track.
        """
        assert isinstance(composable, topology.Composable)
        assert isinstance(composable.expand(), pipeline.Segment)

    def test_noncomposable(self, composable: topology.Composable):
        """Testing noncomposable composition.
        """
        with pytest.raises(ValueError):
            _ = composable >> 1

    def test_self(self, composable: topology.Composable):
        """Testing self composition.
        """
        with pytest.raises(error.Topology):
            _ = composable >> composable

    def test_nonlinear(self, composable: topology.Composable, operator: topology.Operator):
        """Testing nonlinear composition.
        """
        expression = composable >> operator
        with pytest.raises(error.Topology):
            _ = expression >> operator


class TestOrigin(Composable):
    """Origin composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Origin composable fixture.
        """
        return topology.Origin()


class TestCompound(Composable):
    """Compound composable unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composable():
        """Compound composable fixture.
        """
        return topology.Compound(topology.Origin(), topology.Origin())

    def test_compound(self, composable: topology.Composable, operator: topology.Operator):
        """Testing linking action.
        """
        expression = composable >> operator
        assert isinstance(expression, topology.Compound)
