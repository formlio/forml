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


class TestComposition:
    """Composition unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def composition(origin: topology.Operator, operator: topology.Operator) -> pipeline.Composition:
        """Composition fixture.
        """
        return pipeline.Composition((origin >> operator).expand())

    def test_composition(self, origin: topology.Operator, operator: topology.Operator):
        """Test the pipeline.
        """
        with pytest.raises(error.Topology):  # contains Future node
            pipeline.Composition(operator.expand())

        pipeline.Composition((origin >> operator).expand())

    def test_shared(self, composition: pipeline.Composition):
        """Test the composition shared nodes.
        """
        assert any(composition.shared)
