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
ForML compiler unit tests.
"""

import pytest

from forml import flow
from forml.io import asset, layout


@pytest.fixture(scope='session')
def node1(actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Worker:
    """Node fixture."""
    return flow.Worker(actor_builder, 1, 1)


@pytest.fixture(scope='session')
def node2(actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Worker:
    """Node fixture."""
    return flow.Worker(actor_builder, 1, 1)


@pytest.fixture(scope='session')
def node3(actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Worker:
    """Node fixture."""
    return flow.Worker(actor_builder, 1, 1)


@pytest.fixture(scope='session')
def segment(node1: flow.Worker, node2: flow.Worker, node3: flow.Worker):
    """Segment fixture."""
    node2[0].subscribe(node1[0])
    node3[0].subscribe(node2[0])
    return flow.Segment(node1)


def test_compile(
    segment: flow.Segment, valid_instance: asset.Instance, node1: flow.Worker, node2: flow.Worker, node3: flow.Worker
):
    """Compiler generate test."""
    flow.compile(segment, valid_instance.state((node1.gid, node2.gid, node3.gid)))
