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
Graph unit tests.
"""
# pylint: disable=no-self-use, protected-access

import pytest

from forml import flow
from forml.flow._graph import port, span
from forml.io import layout


class TestTraversal:
    """Segment traversal tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def worker(simple: flow.Worker, multi: flow.Worker) -> flow.Worker:
        """Flow fixture."""
        simple[0].subscribe(multi[0])
        return multi

    def test_acyclic(self, worker: flow.Worker, simple: flow.Worker):
        """Test cycle detection."""
        worker[0].subscribe(simple[0])
        with pytest.raises(span.Traversal.Cyclic):  # cyclic flow
            span.Traversal(worker).tail()

    def test_copy(self, worker: flow.Worker, simple: flow.Worker, multi: flow.Worker):
        """Copy test."""
        copy = span.Traversal(worker).copy(simple)
        assert copy[simple].gid == simple.gid
        assert copy[multi].gid == multi.gid

        # copy single-node segment
        future = flow.Future()
        copy = span.Traversal(future).copy(future)
        assert future in copy

    def test_each(self, worker: flow.Worker, simple: flow.Worker, multi: flow.Worker):
        """Each test."""

        def check(node: flow.Worker) -> None:
            """Each step assertion."""
            assert node is expected.pop()

        expected = [simple, multi]
        span.Traversal(worker).each(simple, check)
        assert not expected


class TestSegment:
    """Segment tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def head(actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Worker:
        """Segment head fixture."""
        return flow.Worker(actor_builder, 1, 1)

    @staticmethod
    @pytest.fixture(scope='function', params=(False, True))
    def segment(
        request,
        head: flow.Worker,
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    ) -> flow.Segment:
        """Segment fixture."""
        flow1 = flow.Worker(actor_builder, 1, 2)
        flow2 = flow.Worker(actor_builder, 2, 1)
        flow1[0].subscribe(head[0])
        flow2[0].subscribe(flow1[0])
        flow2[1].subscribe(flow1[1])
        if request.param:  # stateful
            flow3 = flow.Worker(actor_builder, 1, 1)
            flow2[0].publish(flow3, port.Train())
        return flow.Segment(head)

    @staticmethod
    @pytest.fixture(scope='function')
    def presegment(segment: flow.Segment, simple: flow.Worker) -> flow.Segment:
        """Fixture containing the segment as its followup segment."""
        segment.subscribe(simple[0])
        return flow.Segment(simple)

    def test_invalid(self, multi: flow.Worker):
        """Testing invalid segment."""
        with pytest.raises(flow.TopologyError):  # not a simple edge node
            flow.Segment(multi)

    def test_copy(self, segment: flow.Segment):
        """Testing copying segment nodes."""
        copy = segment.copy()
        assert copy._head.gid == segment._head.gid

    def test_pubsub(self, segment: flow.Segment, simple: flow.Worker, multi: flow.Worker):
        """Testing segment publishing."""
        multi.train(segment.publisher, segment.publisher)
        segment.subscribe(simple[0])
        assert flow.Segment(simple)._tail is segment._tail

    def test_follows(self, segment: flow.Segment, presegment: flow.Segment):
        """Testing subsegment checking."""
        assert segment.follows(segment)
        assert not presegment.follows(segment)
        assert segment.follows(presegment)

    def test_root(self, segment: flow.Segment, presegment: flow.Segment):
        """Test the root segment selector."""
        assert flow.Segment.root(segment, presegment) is flow.Segment.root(presegment, segment) is presegment
