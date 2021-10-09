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
Launch support components.
"""
import logging
import typing

from forml import io
from forml import project as prj
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.runtime import asset

from . import _agent

LOGGER = logging.getLogger(__name__)


class Registry:
    """Registry util handle."""

    def __init__(self, registry: typing.Union[provcfg.Registry, asset.Registry]):
        if isinstance(registry, provcfg.Registry):
            registry = asset.Registry[registry.reference](**registry.params)
        self._asset: asset.Directory = asset.Directory(registry)

    def assets(
        self, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str]
    ) -> asset.Instance:
        """Create the assets instance of given registry item.

        Args:
            project: Item's project.
            lineage: Item's lineage.
            generation: Item's generation.

        Returns:
            Asset instance.
        """
        return asset.Instance(project, lineage, generation, self._asset)

    def publish(self, project: str, package: prj.Package) -> None:
        """Publish new package into the registry.

        Args:
            project: Name of project to publish the package into.
            package: Package to be published.
        """
        self._asset.get(project).put(package)

    def list(self, project: typing.Optional[str], lineage: typing.Optional[str]) -> typing.Iterable[asset.Level.Key]:
        """Repository listing subcommand.

        Args:
            project: Name of project to be listed.
            lineage: Lineage version to be listed.

        Returns:
            Listing of the given registry level.
        """
        level = self._asset
        if project:
            level = level.get(project)
            if lineage:
                level = level.get(lineage)
        return level.list()


class Feeds:
    """Feed pool and util handle."""

    def __init__(self, *configs: typing.Union[provcfg.Feed, io.Feed]):
        self._pool: io.Importer = io.Importer(*configs)

    def match(self, query: dsl.Query) -> io.Feed:
        """Select the feed that can provide for given query.

        Args:
            query: ETL query to be run against the required feed.

        Returns:
            Feed that's able to provide data for the given query.
        """
        return self._pool.match(query)

    def list(self):
        """List the sources provided by given feed.

        Returns:
        """
        raise NotImplementedError()


class Launcher:
    """Runner handle."""

    def __init__(self, provider: provcfg.Runner, assets: asset.Instance, feeds: Feeds, sink: 'io.Exporter'):
        self._provider: provcfg.Runner = provider
        self._assets: asset.Instance = assets
        self._feeds: Feeds = feeds
        self._sink: io.Exporter = sink

    @property
    def train(self) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], None]:
        """Return the train handler.

        Returns:
            Train runner.
        """
        return self(self._assets.project.source.extract.train).train

    @property
    def apply(self) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], None]:
        """Return the apply handler.

        Returns:
            Train handler.
        """
        return self(self._assets.project.source.extract.apply, self._sink.apply).apply

    @property
    def train_eval(self) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], None]:
        """Return the eval handler.

        Returns:
            Eval runner.
        """
        return self(self._assets.project.source.extract.train, self._sink.eval).train_eval

    @property
    def apply_eval(self) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], None]:
        """Return the eval handler.

        Returns:
            Eval runner.
        """
        return self(self._assets.project.source.extract.train, self._sink.eval).apply_eval

    @property
    def tune(self) -> typing.Callable[[typing.Optional[dsl.Native], typing.Optional[dsl.Native]], None]:
        """Return the tune handler.

        Returns:
            Tune handler
        """
        raise NotImplementedError()

    def __call__(self, query: dsl.Query, sink: typing.Optional[io.Sink] = None) -> _agent.Runner:
        return _agent.Runner[self._provider.reference](
            self._assets, self._feeds.match(query), sink, **self._provider.params
        )


class Platform:
    """Handle to the runtime functions representing a ForML platform."""

    def __init__(
        self,
        runner: typing.Optional[typing.Union[provcfg.Runner, str]] = None,
        registry: typing.Optional[typing.Union[provcfg.Registry, asset.Registry]] = None,
        feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, io.Feed]]] = None,
        sink: typing.Optional[typing.Union[provcfg.Sink.Mode, str, io.Sink]] = None,
    ):
        if isinstance(runner, str):
            runner = provcfg.Runner.resolve(runner)
        self._runner: provcfg.Runner = runner or provcfg.Runner.default
        self._registry: Registry = Registry(registry or provcfg.Registry.default)
        self._feeds: Feeds = Feeds(*(feeds or provcfg.Feed.default))
        self._sink: io.Exporter = io.Exporter(sink or provcfg.Sink.Mode.default)

    def launcher(
        self,
        project: typing.Optional[str],
        lineage: typing.Optional[str] = None,
        generation: typing.Optional[str] = None,
    ) -> Launcher:
        """Get a runner handle for given project/lineage/generation.

        Args:
            project: Project to run.
            lineage: Lineage to run.
            generation: Generation to run.

        Returns:
            Runner handle.
        """
        return Launcher(self._runner, self._registry.assets(project, lineage, generation), self._feeds, self._sink)

    @property
    def registry(self) -> Registry:
        """Registry handle getter.

        Returns:
            Registry handle.
        """
        return self._registry

    @property
    def feeds(self) -> Feeds:
        """Feeds handle getter.

        Returns:
            Feeds handle.
        """
        return self._feeds
