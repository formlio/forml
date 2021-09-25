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
from forml.flow import error
from forml.flow._graph import port, span


class TestTraversal:
    """Path traversal tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def worker(simple: flow.Worker, multi: flow.Worker) -> flow.Worker:
        """Flow fixture."""
        simple[0].subscribe(multi[0])
        return multi

    def test_acyclic(self, worker, simple: flow.Worker):
        """Test cycle detection."""
        worker[0].subscribe(simple[0])
        with pytest.raises(span.Traversal.Cyclic):  # cyclic flow
            span.Traversal(worker).tail()

    def test_copy(self, worker, simple: flow.Worker, multi: flow.Worker):
        """Copy test."""
        copy = span.Traversal(worker).copy(simple)
        assert copy[simple].gid == simple.gid
        assert copy[multi].gid == multi.gid

    def test_each(self, worker, simple: flow.Worker, multi: flow.Worker):
        """Each test."""

        def check(node: flow.Worker) -> None:
            """Each step assertion."""
            assert node is expected.pop()

        expected = [simple, multi]
        span.Traversal(worker).each(simple, check)
        assert not expected


class TestPath:
    """Path tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def head(spec: flow.Spec) -> flow.Worker:
        """Path head fixture."""
        return flow.Worker(spec, 1, 1)

    @staticmethod
    @pytest.fixture(scope='function', params=(False, True))
    def path(request, head: flow.Worker, spec: flow.Spec) -> span.Path:
        """Path fixture."""
        flow1 = flow.Worker(spec, 1, 2)
        flow2 = flow.Worker(spec, 2, 1)
        flow1[0].subscribe(head[0])
        flow2[0].subscribe(flow1[0])
        flow2[1].subscribe(flow1[1])
        if request.param:  # stateful
            flow3 = flow.Worker(spec, 1, 1)
            flow2[0].publish(flow3, port.Train())
        return span.Path(head)

    @staticmethod
    @pytest.fixture(scope='function')
    def superpath(path: span.Path, simple: flow.Worker) -> span.Path:
        """Fixture containing the path as of its sub-path."""
        path.subscribe(simple[0])
        return span.Path(simple)

    def test_invalid(self, multi: flow.Worker):
        """Testing invalid path."""
        with pytest.raises(error.Topology):  # not a simple edge node
            span.Path(multi)

    def test_copy(self, path: span.Path):
        """Testing copying path nodes."""
        copy = path.copy()
        assert copy._head.gid == path._head.gid

    def test_pubsub(self, path: span.Path, simple: flow.Worker, multi: flow.Worker):
        """Testing path publishing."""
        multi.train(path.publisher, path.publisher)
        path.subscribe(simple[0])
        assert span.Path(simple)._tail is path._tail

    def test_subpath(self, path: span.Path, superpath: span.Path):
        """Testing subpath checking."""
        assert path.issubpath(path)
        assert not superpath.issubpath(path)
        assert path.issubpath(superpath)

    def test_root(self, path: span.Path, superpath: span.Path):
        """Test the root path selector."""
        assert span.Path.root(path, superpath) is span.Path.root(superpath, path) is superpath
