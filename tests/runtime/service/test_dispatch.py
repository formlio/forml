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
Service runtime dispatch tests.
"""
import json
import pickle

import pytest

import forml
from forml import application as appmod
from forml import io
from forml.io import asset, layout
from forml.runtime._service import dispatch


class TestDealer:
    """Dealer unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    async def dealer(feed_instance: io.Feed) -> dispatch.Dealer:
        """Dealer fixture."""
        dealer = dispatch.Dealer(io.Importer(feed_instance), processes=3)
        yield dealer
        dealer.shutdown()

    async def test_call(
        self,
        dealer: dispatch.Dealer,
        valid_instance: asset.Instance,
        testset_entry: layout.Entry,
        generation_prediction: layout.Array,
    ):
        """Dealer call test."""
        outcome = await dealer(valid_instance, testset_entry)
        assert tuple(outcome.data) == generation_prediction


class TestWrapper:
    """Wrapper unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    async def wrapper(inventory: asset.Inventory, registry: asset.Registry) -> dispatch.Wrapper:
        """Wrapper fixture."""
        wrapper = dispatch.Wrapper(inventory, registry, max_workers=3)
        yield wrapper
        wrapper.shutdown()

    @staticmethod
    @pytest.fixture(scope='function')
    def query(
        descriptor: appmod.Descriptor, valid_instance: asset.Instance, testset_request: layout.Request
    ) -> dispatch.Wrapper.Query:
        """Query fixture."""
        return dispatch.Wrapper.Query(
            descriptor, valid_instance, testset_request.accept, descriptor.receive(testset_request)
        )

    async def test_extract(
        self,
        wrapper: dispatch.Wrapper,
        application: str,
        testset_request: layout.Request,
        testset_entry: layout.Entry,
        valid_instance: asset.Instance,
    ):
        """Extract test."""
        query = await wrapper.extract(application, testset_request, None)
        assert query.descriptor.name == application
        assert query.decoded.entry == testset_entry
        assert valid_instance == query.instance

    async def test_invalid(
        self,
        wrapper: dispatch.Wrapper,
        testset_request: layout.Request,
    ):
        """Invalid request test."""
        with pytest.raises(forml.MissingError, match='Application foobar not found in'):
            await wrapper.extract('foobar', testset_request, None)

    async def test_respond(
        self,
        wrapper: dispatch.Wrapper,
        query: dispatch.Wrapper.Query,
        testset_outcome: layout.Outcome,
        generation_prediction: layout.Array,
    ):
        """Respond test."""
        payload = await wrapper.respond(query, testset_outcome)
        assert tuple(v for r in json.loads(payload.data) for v in r.values()) == generation_prediction

    @staticmethod
    @pytest.fixture(scope='function')
    def frozen(registry: asset.Registry) -> dispatch.Wrapper.Frozen:
        """Frozen registry fixture."""
        return dispatch.Wrapper.Frozen(registry)

    def test_frozen(self, frozen: dispatch.Wrapper.Frozen, registry: asset.Registry):
        """Frozen registry tests."""
        with pytest.raises(TypeError, match='Frozen registry is immutable'):
            frozen.push(None)
        with pytest.raises(TypeError, match='Frozen registry is immutable'):
            frozen.write(None, None, None, None)
        with pytest.raises(TypeError, match='Frozen registry is immutable'):
            frozen.close(None, None, None, None)
        assert pickle.loads(pickle.dumps(frozen)) == frozen
        assert frozen == registry
