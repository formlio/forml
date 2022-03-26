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
import abc
import asyncio
import logging
import typing

import forml
from forml import io
from forml.conf.parsed import provider as provcfg  # pylint: disable=unused-import
from forml.io import layout
from forml.runtime import asset

from . import dispatch

LOGGER = logging.getLogger(__name__)


class Engine:
    """Serving engine implementation."""

    def __init__(
        self,
        inventory: asset.Inventory,
        registry: asset.Registry,
        feeds: io.Importer,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._wrapper: dispatch.Wrapper = dispatch.Wrapper(inventory, registry, processes, loop)
        self._dealer: dispatch.Dealer = dispatch.Dealer(feeds, processes, loop)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._wrapper.shutdown()
        self._dealer.discard()

    async def stats(self) -> layout.Stats:
        """Get the collected stats report."""

    async def apply(self, application: str, request: layout.Request) -> layout.Response:
        """Engine predict entrypoint.

        Args:
            application: Application unique name.
            request: Application request instance.

        Returns:
            Serving result response.
        """
        query = await self._wrapper.extract(application, request, layout.Stats())
        outcome = await self._dealer(query.instance, query.decoded.entry)
        return await self._wrapper.respond(query, outcome)


class Gateway(forml.Provider, default=provcfg.Gateway.default, path=provcfg.Gateway.path):
    """Top-level serving gateway abstraction."""

    def __init__(
        self,
        inventory: typing.Optional[asset.Inventory] = None,
        registry: typing.Optional[asset.Registry] = None,
        feeds: typing.Optional[io.Importer] = None,
        **_,
    ):
        if not inventory:
            inventory = asset.Inventory()
        if not registry:
            registry = asset.Registry()
        if not feeds:
            feeds = io.Importer(io.Feed())
        self._engine: Engine = Engine(inventory, registry, feeds)

    @abc.abstractmethod
    def run(
        self,
        apply: typing.Callable[[str, layout.Request], typing.Awaitable[layout.Response]],
        stats: typing.Callable[[], typing.Awaitable[layout.Stats]],
    ) -> None:
        """Serving loop."""

    def main(self) -> None:
        """Frontend main method."""
        with self._engine as engine:
            self.run(engine.apply, engine.stats)
