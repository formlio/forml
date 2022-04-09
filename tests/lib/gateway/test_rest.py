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
ForML rest gateway unit tests.
"""
# pylint: disable=no-self-use
import contextlib
import typing

import pytest
from starlette import applications, testclient

from forml import io, project
from forml.io import asset, layout
from forml.lib.gateway import rest


class TestGateway:
    """Gateway unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    async def client(
        inventory: asset.Inventory, registry: asset.Registry, feed_instance: io.Feed
    ) -> testclient.TestClient:
        """Gateway client fixture."""

        def server(app: applications.Starlette, **_) -> None:
            """Wrapper for extracting the application."""
            nonlocal client
            client = testclient.TestClient(app)

        client: typing.ContextManager[testclient.TestClient] = contextlib.nullcontext()
        with rest.Gateway(inventory, registry, io.Importer(feed_instance), processes=3, server=server), client:
            yield client

    def test_stats(self, client: testclient.TestClient):
        """Test the stats endpoint."""
        response = client.get(rest.Stats.PATH)
        assert response.status_code == 200

    def test_apply(
        self,
        client: testclient.TestClient,
        descriptor: type[project.Descriptor],
        testset_request: layout.Request,
        generation_prediction: layout.Array,
    ):
        """Test the application predict endpoint."""
        response = client.post(
            f'/{descriptor.application}',
            data=testset_request.payload,
            headers={'content-type': descriptor.JSON.header, 'accept': descriptor.JSON.header},
        )
        assert response.status_code == 200
        assert tuple(v for r in response.json() for v in r.values()) == generation_prediction

    def test_invalid(self, client: testclient.TestClient):
        """Test invalid requests."""
        response = client.get('/foobar')
        assert response.status_code == 405
        response = client.post('/foobar')
        assert response.status_code == 404
