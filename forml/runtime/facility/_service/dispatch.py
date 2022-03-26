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
import logging
import typing
import uuid
from concurrent import futures

from forml import io
from forml import project as prjmod
from forml.io import layout
from forml.runtime import asset

from . import prediction

LOGGER = logging.getLogger(__name__)


class Dealer:
    """Pool of prediction executors."""

    def __init__(
        self,
        feeds: io.Importer,
        processes: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._feeds: io.Importer = feeds
        self._processes: typing.Optional[int] = processes
        self._loop: typing.Optional[asyncio.AbstractEventLoop] = loop
        self._cache: dict[asset.Instance, prediction.Executor] = {}

    def __call__(self, instance: asset.Instance, entry: layout.Entry) -> asyncio.Future[layout.Outcome]:
        if instance not in self._cache:
            LOGGER.info('Spawning new prediction executor')
            executor = prediction.Executor(
                instance, self._feeds.match(instance.project.source.extract.apply), self._processes
            )
            executor.start()
            self._cache[instance] = executor
        outcome = self._cache[instance].apply(entry)
        return asyncio.wrap_future(outcome, loop=self._loop)

    def discard(self) -> None:
        """Stop and drop all the cached executors."""
        for executor in self._cache.values():
            executor.stop()
        self._cache.clear()


class Wrapper:
    """(Un)Wrapper of engine requests and their responses."""

    class Frozen(asset.Registry):
        """Registry proxy blocking all write attempts."""

        ERROR = TypeError('Frozen registry is immutable')

        def __init__(self, registry: asset.Registry):
            super().__init__()
            self._registry: asset.Registry = registry

        def __hash__(self):
            return hash(self._registry)

        def __eq__(self, other):
            return self._registry == other

        def projects(self) -> typing.Iterable[typing.Union[str, asset.Project.Key]]:
            return self._registry.projects()

        def releases(self, project: asset.Project.Key) -> typing.Iterable[typing.Union[str, asset.Release.Key]]:
            return self._registry.releases(project)

        def generations(
            self, project: asset.Project.Key, release: asset.Release.Key
        ) -> typing.Iterable[typing.Union[str, int, asset.Generation.Key]]:
            return self._registry.generations(project, release)

        def pull(self, project: asset.Project.Key, release: asset.Release.Key) -> prjmod.Package:
            return self._registry.pull(project, release)

        def push(self, package: prjmod.Package) -> None:
            raise self.ERROR

        def read(
            self,
            project: asset.Project.Key,
            release: asset.Release.Key,
            generation: asset.Generation.Key,
            sid: uuid.UUID,
        ) -> bytes:
            return self._registry.read(project, release, generation, sid)

        def write(self, project: asset.Project.Key, release: asset.Release.Key, sid: uuid.UUID, state: bytes) -> None:
            raise self.ERROR

        def open(
            self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
        ) -> asset.Tag:
            return self._registry.open(project, release, generation)

        def close(
            self,
            project: asset.Project.Key,
            release: asset.Release.Key,
            generation: asset.Generation.Key,
            tag: asset.Tag,
        ) -> None:
            raise self.ERROR

    class Query(typing.NamedTuple):
        """Case class for holding query attributes."""

        descriptor: prjmod.Descriptor
        instance: asset.Instance
        accept: tuple[layout.Encoding]
        decoded: layout.Request.Decoded

    def __init__(
        self,
        inventory: asset.Inventory,
        registry: asset.Registry,
        max_workers: typing.Optional[int] = None,
        loop: typing.Optional[asyncio.AbstractEventLoop] = None,
    ):
        self._inventory: asset.Inventory = inventory
        self._registry: asset.Directory = asset.Directory(self.Frozen(registry))
        self._processes: futures.ProcessPoolExecutor = futures.ProcessPoolExecutor(max_workers)
        self._threads: futures.ThreadPoolExecutor = futures.ThreadPoolExecutor(max_workers)
        self._loop: asyncio.AbstractEventLoop = loop or asyncio.get_running_loop()

    @functools.lru_cache
    def _get_descriptor(self, application: str) -> prjmod.Descriptor:
        """Get the application descriptor.

        Args:
            application: Application unique name.

        Returns:
            Application descriptor.
        """
        return self._inventory.get(application)

    @staticmethod
    def _dispatch(
        descriptor: prjmod.Descriptor,
        registry: asset.Directory,
        request: layout.Request,
        stats: layout.Stats,
    ) -> tuple[asset.Instance, layout.Request.Decoded]:
        """Helper for request decoding and model selection.

        Args:
            descriptor: Application descriptor to be used for dispatching.
            registry: Model registry to select from.
            request: Native input request.
            stats: Actual system stats provided for the dispatcher to potentially use for model selection.

        Returns:
            Asset instance object and decoded version of the serving request.
        """
        decoded = descriptor.decode(request)
        return descriptor.select(registry, decoded.scope, stats), decoded

    async def extract(self, application: str, request: layout.Request, stats: layout.Stats) -> 'Wrapper.Query':
        """Extract the query parameters from the given request object belonging to the particular application.

        Args:
            application: Name of application/descriptor to use for dispatching.
            request: Native input request.
            stats: Actual system stats provided for the dispatcher to potentially use for model selection.

        Returns:
            Extracted query parameters.
        """
        descriptor = await self._loop.run_in_executor(self._threads, self._get_descriptor, application)
        instance, decoded = await self._loop.run_in_executor(
            self._processes, self._dispatch, descriptor, self._registry, request, stats
        )
        return self.Query(descriptor, instance, request.accept, decoded)

    async def respond(self, query: 'Wrapper.Query', outcome: layout.Outcome) -> layout.Response:
        """Encode the given outcome into a native response.

        Args:
            query: Query parameters as returned from extract.
            outcome: Prediction outcome.

        Returns:
            Native encoded response.
        """
        return await self._loop.run_in_executor(
            self._processes,
            query.descriptor.encode,
            outcome,
            query.accept,
            query.decoded.scope,
        )

    def shutdown(self) -> None:
        """Terminate the executors."""
        self._processes.shutdown()
        self._threads.shutdown()
