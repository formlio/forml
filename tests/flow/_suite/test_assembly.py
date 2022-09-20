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
import typing
import uuid

import pytest

from forml import flow, io, project
from forml.flow._graph import atomic, span
from forml.flow._suite import assembly, member


class TestTrunk:
    """Segment unit tests."""

    def test_new(self):
        """Test segment setup."""
        assert all(
            isinstance(m, span.Segment) for m in assembly.Trunk(span.Segment(atomic.Future()), atomic.Future(), None)
        )


class TestComposition:
    """Composition unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def composition(
        feed_instance: io.Feed, project_components: project.Components, sink_instance: io.Sink
    ) -> assembly.Composition:
        """Composition fixture."""
        return (
            assembly.Composition.builder(
                feed_instance.load(project_components.source.extract), project_components.source.transform
            )
            .via(project_components.pipeline)
            .build(sink_instance.save(None))
        )

    def test_invalid(self, origin: member.Operator, operator: member.Operator):
        """Test the pipeline."""
        assembly.Composition.builder(origin).via(operator).build(origin)

        with pytest.raises(flow.TopologyError, match='Future nodes in segment'):
            assembly.Composition.builder(flow.Origin()).via(flow.Origin()).build()

        with pytest.raises(flow.TopologyError, match='Illegal use of stateful node'):
            assembly.Composition.builder(operator).build()

        with pytest.raises(flow.TopologyError, match='Illegal use of stateful node'):
            assembly.Composition.builder(origin).build(operator)

    def test_persistent(self, composition: assembly.Composition, stateful_nodes: typing.Sequence[uuid.UUID]):
        """Test the composition persistent nodes."""
        assert len(composition.persistent) == len(stateful_nodes)
