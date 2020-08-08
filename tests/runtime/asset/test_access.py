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

from forml.runtime.asset import access
from forml.runtime.asset.directory import generation as genmod


class TestAssets:
    """Assets unit tests.
    """
    def test_tag(self, valid_assets: access.Assets, tag: genmod.Tag):
        """Test default empty lineage generation retrieval.
        """
        assert valid_assets.tag is tag


class TestState:
    """State unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def assets(valid_assets: access.Assets, nodes: typing.Sequence[uuid.UUID]) -> access.State:
        """State fixture.
        """
        return valid_assets.state(nodes)

    def test_load(self, assets: access.State, nodes: typing.Sequence[uuid.UUID],
                  states: typing.Mapping[uuid.UUID, bytes]):
        """Test state loading.
        """
        for node, value in zip(nodes, states.values()):
            assert assets.load(node) == value
