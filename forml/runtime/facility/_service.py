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

import functools
import typing

from forml import io, project
from forml.io import dsl, layout
from forml.lib.runner import pyfunc
from forml.runtime import asset


class Sink(io.Sink):
    """Dummy sink that only returns combo of the schema and the payload."""

    @classmethod
    def consumer(cls, schema: dsl.Schema, **kwargs: typing.Any) -> io.Consumer:
        return lambda d: (schema, d)


class Engine:
    """Serving engine implementation."""

    RUNNER_SLOTS = 32
    """How many runner instances to hold in cache."""

    def __init__(
        self, inventory: asset.Inventory, registry: asset.Registry, feeds: io.Importer, runner_slots: int = 32
    ):
        @functools.lru_cache(runner_slots)
        def get_runner_cached(instance: asset.Instance) -> pyfunc.Runner:
            """Helper for creating a runner for the given instance.

            Args:
                instance: Product assets.

            Returns:
                Instance of thePyFunc runner.
            """
            return pyfunc.Runner(instance, feeds.match(instance.project.source.extract.apply), sink)

        sink: Sink = Sink()
        self._inventory: asset.Inventory = inventory
        self._registry: asset.Registry = registry
        self._get_runner: typing.Callable[[asset.Instance], pyfunc.Runner] = get_runner_cached

    @functools.lru_cache
    def _get_descriptor(self, application: str) -> project.Descriptor:
        """Get the application descriptor.

        Args:
            application: Application unique name.

        Returns:
            Application descriptor.
        """
        return self._inventory.get(application)

    def apply(self, application: str, request: layout.Request) -> layout.Response:
        """Engine predict entrypoint.

        Args:
            application: Application unique name.
            request: Application request instance.

        Returns:
            Serving result response.
        """
        descriptor = self._get_descriptor(application)
        instance = descriptor.select(self._registry)
        result = self._get_runner(instance).call(descriptor.decode(request))
        return descriptor.encode(result, request.accept)
