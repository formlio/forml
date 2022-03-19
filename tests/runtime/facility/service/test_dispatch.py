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
Service facility dispatch tests.
"""
# pylint: disable=no-self-use
import json

import pytest

from forml import io, project
from forml.io import layout
from forml.runtime import asset
from forml.runtime.facility._service import dispatch


class TestRegistry:
    """Registry unit tests."""


class TestDealer:
    """Dealer unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    async def dealer(feed_instance: io.Feed) -> dispatch.Dealer:
        """Dealer fixture."""
        dealer = dispatch.Dealer(io.Importer(feed_instance), processes=3)
        yield dealer
        dealer.discard()

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
        descriptor: project.Descriptor, valid_instance: asset.Instance, testset_request: layout.Request
    ) -> dispatch.Wrapper.Query:
        """Query fixture."""
        return dispatch.Wrapper.Query(
            descriptor, valid_instance, testset_request.accept, descriptor.decode(testset_request)
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
        assert query.descriptor.application == application
        assert query.decoded.entry == testset_entry
        assert query.instance.tag == valid_instance.tag

    async def test_respond(
        self,
        wrapper: dispatch.Wrapper,
        query: dispatch.Wrapper.Query,
        testset_outcome: layout.Outcome,
        generation_prediction: layout.Array,
    ):
        """Respond test."""
        response = await wrapper.respond(query, testset_outcome)
        assert tuple(v for r in json.loads(response.payload) for v in r.values()) == generation_prediction
