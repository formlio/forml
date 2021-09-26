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

from forml.flow import error
from forml.flow._graph import node, span
from forml.flow._suite import assembly, member


class TestTrunk:
    """Segment unit tests."""

    def test_new(self):
        """Test segment setup."""
        assert all(isinstance(m, span.Path) for m in assembly.Trunk(span.Path(node.Future()), node.Future(), None))


class TestComposition:
    """Composition unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def composition(origin: member.Operator, operator: member.Operator) -> assembly.Composition:
        """Composition fixture."""
        return assembly.Composition((origin >> operator).expand())

    def test_composition(self, origin: member.Operator, operator: member.Operator):
        """Test the pipeline."""
        with pytest.raises(error.Topology):  # contains Future node
            assembly.Composition(operator.expand())

        assembly.Composition((origin >> operator).expand())

    def test_shared(self, composition: assembly.Composition):
        """Test the composition shared nodes."""
        assert any(composition.shared)
