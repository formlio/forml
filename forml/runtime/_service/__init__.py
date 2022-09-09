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
import logging
import typing

from forml import io, provider
from forml import runtime as runmod
from forml import setup
from forml.io import asset, layout

from .. import _perf
from . import dispatch

if typing.TYPE_CHECKING:
    import asyncio

    from forml import runtime  # pylint: disable=reimported


LOGGER = logging.getLogger(__name__)


class Engine:
    """Serving engine implementation."""

    def __init__(
        self,
        inventory: asset.Inventory,
        registry: asset.Registry,
        feeds: io.Importer,
        processes: typing.Optional[int] = None,
        loop: typing.Optional['asyncio.AbstractEventLoop'] = None,
    ):
        self._wrapper: dispatch.Wrapper = dispatch.Wrapper(inventory, registry, processes, loop)
        self._dealer: dispatch.Dealer = dispatch.Dealer(feeds, processes, loop)

    def shutdown(self):
        """Terminate the engine."""
        self._wrapper.shutdown()
        self._dealer.shutdown()

    async def stats(self) -> 'runtime.Stats':
        """Get the collected stats report.

        Returns:
            Performance metrics report.

        Todo: Implement true stats collection.
        """
        return runmod.Stats()

    async def apply(self, application: str, request: layout.Request) -> layout.Response:
        """Engine predict entrypoint.

        Args:
            application: Application unique name.
            request: Application request instance.

        Returns:
            Serving result response.
        """
        query = await self._wrapper.extract(application, request, _perf.Stats())
        outcome = await self._dealer(query.instance, query.decoded.entry)
        return await self._wrapper.respond(query, outcome)


class Gateway(provider.Service, default=setup.Gateway.default, path=setup.Gateway.path):
    """Top-level serving gateway abstraction.

    Args:
        inventory: Inventory of applications to be served
                   (default as per the platform configuration).
        registry: Model registry of project artifacts to be served
                  (default as per the platform configuration).
        feeds: Feeds to be used for potential feature augmentation
               (default as per the platform configuration).
        processes: Process pool size for each model sandbox.
        loop: Explicit event loop instance.
        kwargs: Additional serving loop keyword arguments passed to the :meth:`run` method.
    """

    def __init__(
        self,
        inventory: typing.Optional['asset.Inventory'] = None,
        registry: typing.Optional['asset.Registry'] = None,
        feeds: typing.Optional['io.Importer'] = None,
        processes: typing.Optional[int] = None,
        loop: typing.Optional['asyncio.AbstractEventLoop'] = None,
        **kwargs,
    ):
        if not inventory:
            inventory = asset.Inventory()
        if not registry:
            registry = asset.Registry()
        if not feeds:
            feeds = io.Importer(io.Feed())
        self._engine: Engine = Engine(inventory, registry, feeds, processes=processes, loop=loop)
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __enter__(self):
        self.run(self._engine.apply, self._engine.stats, **self._kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.shutdown()

    @classmethod
    @abc.abstractmethod
    def run(
        cls,
        apply: typing.Callable[[str, 'layout.Request'], typing.Awaitable['layout.Response']],
        stats: typing.Callable[[], typing.Awaitable['runtime.Stats']],
        **kwargs,
    ) -> None:
        """Serving loop implementation.

        Args:
            apply: Prediction request handler provided by the engine.
                   The handler expects two parameters - the target *application name* and the
                   *prediction request*.
            stats: Stats producer callback provided by the engine.
            kwargs: Additional keyword arguments provided via the constructor.
        """
        raise NotImplementedError()

    def main(self) -> None:
        """Frontend main method."""
        with self:
            pass
