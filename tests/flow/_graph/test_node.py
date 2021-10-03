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
Graph node unit tests.
"""
# pylint: disable=no-self-use

import abc

import pytest

from forml import flow
from forml.flow._graph import port


class Atomic(metaclass=abc.ABCMeta):
    """Base class for node tests."""

    @staticmethod
    @abc.abstractmethod
    def node(spec: flow.Spec) -> flow.Atomic:
        """Node fixture."""

    def test_copy(self, node: flow.Atomic):
        """Test for node copy method."""
        assert isinstance(node.fork(), flow.Atomic)

    def test_subscribe(self, node: flow.Atomic, simple: flow.Worker):
        """Test node subscribing."""
        simple[0].subscribe(node[0])
        assert any(simple is s.node and s.port == port.Apply(0) for s in node.output[0])
        assert port.Apply(0) in simple.input
        with pytest.raises(flow.TopologyError):  # self subscription
            simple[0].subscribe(node[0])

    def test_publish(self, node: flow.Atomic, simple: flow.Worker):
        """Test node publishing."""
        node[0].publish(simple, port.Train())
        assert any(simple is s.node and s.port is port.Train() for s in node.output[0])
        assert port.Train() in simple.input
        with pytest.raises(flow.TopologyError):  # already subscribed
            node[0].publish(simple, port.Train())
        with pytest.raises(flow.TopologyError):  # self subscription
            node[0].publish(node, port.Apply(0))
        with pytest.raises(flow.TopologyError):  # apply-train collision
            node[0].publish(simple, port.Apply(0))
        with pytest.raises(flow.TopologyError):  # trained node publishing
            node[0].subscribe(simple[0])


class TestWorker(Atomic):
    """Specific tests for the worker node."""

    @staticmethod
    @pytest.fixture(scope='function')
    def node(spec: flow.Spec) -> flow.Worker:
        """Node fixture."""
        return flow.Worker(spec, 1, 1)

    def test_train(self, node: flow.Worker, simple: flow.Worker, multi: flow.Worker):
        """Test train subscription"""
        node.train(multi[0], multi[1])
        assert any(node is s.node and s.port == port.Train() for s in multi.output[0])
        assert any(node is s.node and s.port == port.Label() for s in multi.output[1])
        assert node.trained
        with pytest.raises(flow.TopologyError):  # train-apply collision
            node[0].subscribe(simple[0])
        with pytest.raises(flow.TopologyError):  # publishing node trained
            multi.train(node[0], node[0])

    def test_fork(self, node: flow.Worker, multi: flow.Worker):
        """Testing node creation."""
        fork = node.fork()
        assert {node, fork} == node.group
        node.train(multi[0], multi[1])
        with pytest.raises(flow.TopologyError):  # Fork train non-exclusive
            fork.train(multi[0], multi[1])

    def test_stateful(self, node: flow.Worker):
        """Test the node statefulness."""
        assert node.stateful

    def test_spec(self, node: flow.Worker, spec: flow.Spec):
        """Test the node spec."""
        assert node.spec is spec


class TestFuture(Atomic):
    """Specific tests for the future node."""

    @staticmethod
    @pytest.fixture(scope='function')
    def node(spec: flow.Spec) -> flow.Future:
        """Node fixture."""
        return flow.Future()

    def test_future(self, node: flow.Future, simple: flow.Worker, multi: flow.Worker):
        """Test future publishing."""
        node[0].subscribe(simple[0])
        node[0].publish(multi, port.Train())
        assert any(multi is s.node and s.port == port.Train() for s in simple.output[0])

    def test_invalid(self, node: flow.Future, multi: flow.Worker):
        """Testing invalid future subscriptions."""
        node[0].publish(multi, port.Train())
        with pytest.raises(flow.TopologyError):  # trained node publishing
            node[0].subscribe(multi[0])
