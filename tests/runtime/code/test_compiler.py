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
# pylint: disable=no-self-use

import pytest

from forml.flow import task
from forml.flow.graph import node, view
from forml.runtime.asset import access
from forml.runtime.code import compiler


@pytest.fixture(scope='session')
def node1(spec: task.Spec) -> node.Worker:
    """Node fixture."""
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def node2(spec: task.Spec) -> node.Worker:
    """Node fixture."""
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def node3(spec: task.Spec) -> node.Worker:
    """Node fixture."""
    return node.Worker(spec, 1, 1)


@pytest.fixture(scope='session')
def path(node1: node.Worker, node2: node.Worker, node3: node.Worker):
    """Path fixture."""
    node2[0].subscribe(node1[0])
    node3[0].subscribe(node2[0])
    return view.Path(node1)


def test_generate(
    path: view.Path, valid_assets: access.Assets, node1: node.Worker, node2: node.Worker, node3: node.Worker
):
    """Compiler generate test."""
    compiler.generate(path, valid_assets.state((node1.gid, node2.gid, node3.gid)))
