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
Service facility tests.
"""
# pylint: disable=no-self-use
import json

import pytest

import forml
from forml import io
from forml.io import asset, layout
from forml.runtime.facility import _service


class TestEngine:
    """Engine unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    async def engine(inventory: asset.Inventory, registry: asset.Registry, feed_instance: io.Feed) -> _service.Engine:
        """Engine fixture."""
        engine = _service.Engine(inventory, registry, io.Importer(feed_instance), processes=3)
        yield engine
        engine.shutdown()

    async def test_apply(
        self,
        engine: _service.Engine,
        application: str,
        testset_request: layout.Request,
        generation_prediction: layout.Array,
    ):
        """Apply unit test."""
        response = await engine.apply(application, testset_request)
        assert tuple(v for r in json.loads(response.payload) for v in r.values()) == generation_prediction

    async def test_invalid(
        self,
        engine: _service.Engine,
        testset_request: layout.Request,
    ):
        """Invalid request test."""
        with pytest.raises(forml.MissingError, match='Application foobar not found in'):
            await engine.apply('foobar', testset_request)
