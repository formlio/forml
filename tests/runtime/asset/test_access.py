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
ForML asset access unit tests.
"""
import typing

# pylint: disable=no-self-use
import uuid

import pytest

from forml.runtime import asset


class TestInstance:
    """Instance unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def clone(valid_instance: asset.Instance) -> asset.Instance:
        """Instance clone fixture."""
        generation: asset.Generation = valid_instance._generation  # pylint: disable=protected-access
        return asset.Instance(
            generation.project.key, generation.lineage.key, generation.key, asset.Directory(generation.registry)
        )

    def test_tag(self, valid_instance: asset.Instance, generation_tag: asset.Tag):
        """Test default empty lineage generation retrieval."""
        assert valid_instance.tag == generation_tag

    def test_equal(self, valid_instance: asset.Instance, clone: asset.Instance):
        """Test instance equality."""
        assert hash(valid_instance) == hash(clone)
        assert valid_instance == clone


class TestState:
    """State unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def state(valid_instance: asset.Instance, stateful_nodes: typing.Sequence[uuid.UUID]) -> asset.State:
        """State fixture."""
        return valid_instance.state(stateful_nodes)

    def test_load(
        self,
        state: asset.State,
        stateful_nodes: typing.Sequence[uuid.UUID],
        generation_states: typing.Mapping[uuid.UUID, bytes],
    ):
        """Test state loading."""
        for node, value in zip(stateful_nodes, generation_states.values()):
            assert state.load(node) == value
