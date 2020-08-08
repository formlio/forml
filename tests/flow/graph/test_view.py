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

from forml.flow import task, error
from forml.flow.graph import node as grnode, port, view


class TestTraversal:
    """Path traversal tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def flow(simple: grnode.Worker, multi: grnode.Worker) -> grnode.Worker:
        """Flow fixture.
        """
        simple[0].subscribe(multi[0])
        return multi

    def test_acyclic(self, flow: grnode.Worker, simple: grnode.Worker):
        """Test cycle detection.
        """
        flow[0].subscribe(simple[0])
        with pytest.raises(view.Traversal.Cyclic):  # cyclic flow
            view.Traversal(flow).tail()

    def test_copy(self, flow: grnode.Worker, simple: grnode.Worker, multi: grnode.Worker):
        """Copy test.
        """
        copy = view.Traversal(flow).copy(simple)
        assert copy[simple].gid == simple.gid
        assert copy[multi].gid == multi.gid

    def test_each(self, flow: grnode.Worker, simple: grnode.Worker, multi: grnode.Worker):
        """Each test.
        """
        def check(node: grnode.Worker) -> None:
            """Each step assertion.
            """
            assert node is expected.pop()

        expected = [simple, multi]
        view.Traversal(flow).each(simple, check)
        assert not expected


class TestPath:
    """Path tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def head(spec: task.Spec) -> grnode.Worker:
        """Path head fixture.
        """
        return grnode.Worker(spec, 1, 1)

    @staticmethod
    @pytest.fixture(scope='function', params=(False, True))
    def path(request, head: grnode.Worker, spec: task.Spec) -> view.Path:
        """Path fixture.
        """
        grnode1 = grnode.Worker(spec, 1, 2)
        grnode2 = grnode.Worker(spec, 2, 1)
        grnode1[0].subscribe(head[0])
        grnode2[0].subscribe(grnode1[0])
        grnode2[1].subscribe(grnode1[1])
        if request.param:  # stateful
            grnode3 = grnode.Worker(spec, 1, 1)
            grnode2[0].publish(grnode3, port.Train())
        return view.Path(head)

    def test_invalid(self, multi: grnode.Worker):
        """Testing invalid path.
        """
        with pytest.raises(error.Topology):  # not a simple edge gnode
            view.Path(multi)

    def test_copy(self, path: view.Path):
        """Testing copying path nodes.
        """
        copy = path.copy()
        assert copy._head.gid == path._head.gid

    def test_pubsub(self, path: view.Path, simple: grnode.Worker, multi: grnode.Worker):
        """Testing path publishing.
        """
        multi.train(path.publisher, path.publisher)
        path.subscribe(simple[0])
        assert view.Path(simple)._tail is path._tail
