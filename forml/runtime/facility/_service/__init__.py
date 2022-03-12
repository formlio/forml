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
Runtime service facility.
"""
import asyncio
import functools
import typing

from forml import io, project
from forml.io import layout
from forml.runtime import asset

from . import worker


class Dispatcher:
    """Caching executor dispatcher."""

    def __init__(
        self,
        feeds: io.Importer,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._feeds: io.Importer = feeds
        self._processes: typing.Optional[int] = processes
        self._loop: typing.Optional[asyncio.AbstractEventLoop] = loop
        self._cache: dict[asset.Instance, worker.Executor] = {}

    def __call__(self, instance: asset.Instance, entry: layout.Entry) -> asyncio.Future[layout.Outcome]:
        if instance not in self._cache:
            executor = worker.Executor(
                instance, self._feeds.match(instance.project.source.extract.apply), self._processes
            )
            self._cache[instance] = executor
        outcome = self._cache[instance].apply(entry)
        return asyncio.wrap_future(outcome, loop=self._loop)


class Engine:
    """Serving engine implementation."""

    def __init__(
        self,
        inventory: asset.Inventory,
        registry: asset.Directory,
        feeds: io.Importer,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._inventory: asset.Inventory = inventory
        self._registry: asset.Directory = registry
        self._dispatcher: Dispatcher = Dispatcher(feeds, processes, loop)

    @functools.lru_cache
    def _get_descriptor(self, application: str) -> project.Descriptor:
        """Get the application descriptor.

        Args:
            application: Application unique name.

        Returns:
            Application descriptor.
        """
        return self._inventory.get(application)

    async def apply(self, application: str, request: layout.Request) -> layout.Response:
        """Engine predict entrypoint.

        Args:
            application: Application unique name.
            request: Application request instance.

        Returns:
            Serving result response.
        """
        descriptor = self._get_descriptor(application)
        entry = descriptor.decode(request)
        instance = descriptor.select(self._registry, entry)
        result = await self._dispatcher(instance, entry)
        return descriptor.encode(result, entry, request.accept)
