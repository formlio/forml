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
Runtime layer.
"""
import abc
import typing

from forml import provider as provmod, error
from forml.conf.parsed import provider as provcfg
from forml.flow import pipeline
from forml.io import feed as feedmod, sink as sinkmod
from forml.io.dsl.struct import frame, kind
from forml.runtime import code
from forml.runtime.asset import persistent, access, directory
from forml.runtime.asset.directory import root
from forml.runtime.code import compiler

if typing.TYPE_CHECKING:
    from forml.project import distribution


class Runner(provmod.Interface, default=provcfg.Runner.default, path=provcfg.Runner.path):
    """Abstract base runner class to be extended by particular runner implementations."""

    def __init__(
        self,
        assets: typing.Optional[access.Assets] = None,
        feed: typing.Optional['feedmod.Provider'] = None,
        sink: typing.Optional['sinkmod.Provider'] = None,
        **_,
    ):
        self._assets: access.Assets = assets or access.Assets()
        self._feed: feedmod.Provider = feed or feedmod.Provider()
        self._sink: typing.Optional[sinkmod.Provider] = sink

    def train(self, lower: typing.Optional['kind.Native'] = None, upper: typing.Optional['kind.Native'] = None) -> None:
        """Run the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._build(lower or self._assets.tag.training.ordinal, upper, self._assets.project.pipeline)
        self._exec(composition.train, self._assets.state(composition.shared, self._assets.tag.training.trigger()))

    def apply(self, lower: typing.Optional['kind.Native'] = None, upper: typing.Optional['kind.Native'] = None) -> None:
        """Run the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        composition = self._build(lower, upper, self._assets.project.pipeline)
        self._exec(composition.apply, self._assets.state(composition.shared))

    def tune(  # pylint: disable=no-self-use
        self, lower: typing.Optional['kind.Native'] = None, upper: typing.Optional['kind.Native'] = None
    ) -> None:
        """Run the tune mode.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        raise error.Missing('Not yet supported')

    def eval(self, lower: typing.Optional['kind.Native'] = None, upper: typing.Optional['kind.Native'] = None) -> None:
        """Run the development model evaluation.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper: Ordinal value as the upper bound for the ETL cycle.
        """
        self._exec(self._evaluation(lower, upper).train)

    def _evaluation(
        self, lower: typing.Optional['kind.Native'] = None, upper: typing.Optional['kind.Native'] = None
    ) -> pipeline.Segment:
        """Return the evaluation pipeline.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns:
            Evaluation pipeline.
        """
        if not self._assets.project.evaluation:
            raise error.Missing('Project not evaluable')
        return self._build(lower, upper, self._assets.project.pipeline >> self._assets.project.evaluation)

    def _build(
        self, lower: typing.Optional['kind.Native'], upper: typing.Optional['kind.Native'], *blocks: pipeline.Segment
    ) -> pipeline.Composition:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns:
            Assembled flow pipeline.
        """
        return pipeline.Composition(
            self._feed.load(self._assets.project.source, lower, upper),
            *(b.expand() for b in blocks),
            self._sink.publish(),
        )

    def _exec(self, path: pipeline.Segment, assets: typing.Optional[access.State] = None) -> None:
        """Execute the given path and assets.

        Args:
            path: Pipeline path.
            assets: Persistent assets to be used.

        Returns:
            Optional return value.
        """
        return self._run(compiler.generate(path, assets))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[code.Symbol]) -> None:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.

        Returns:
            Optional pipeline return value.
        """


class Platform:
    """Handle to the runtime functions representing a ForML platform."""

    class Launcher:
        """Runner handle."""

        def __init__(
            self, provider: provcfg.Runner, assets: access.Assets, feeds: 'Platform.Feeds', sink: sinkmod.Handle
        ):
            self._provider: provcfg.Runner = provider
            self._assets: access.Assets = assets
            self._feeds: Platform.Feeds = feeds
            self._sink: sinkmod.Handle = sink

        @property
        def train(self) -> typing.Callable[[typing.Optional['kind.Native'], typing.Optional['kind.Native']], None]:
            """Return the train handler.

            Returns:
                Train runner.
            """
            return self(self._assets.project.source.extract.train, self._sink.train).train

        @property
        def apply(self) -> typing.Callable[[typing.Optional['kind.Native'], typing.Optional['kind.Native']], None]:
            """Return the apply handler.

            Returns:
                Train handler.
            """
            return self(self._assets.project.source.extract.apply, self._sink.apply).apply

        @property
        def eval(self) -> typing.Callable[[typing.Optional['kind.Native'], typing.Optional['kind.Native']], None]:
            """Return the eval handler.

            Returns:
                Eval runner.
            """
            return self(self._assets.project.source.extract.train, self._sink.eval).eval

        @property
        def tune(self) -> typing.Callable[[typing.Optional['kind.Native'], typing.Optional['kind.Native']], None]:
            """Return the tune handler.

            Returns:
                Tune handler
            """
            raise NotImplementedError()

        def __call__(self, query: 'frame.Query', sink: 'sinkmod.Provider') -> Runner:
            return Runner[self._provider.reference](
                self._assets, self._feeds.match(query), sink, **self._provider.params
            )

    class Registry:
        """Registry util handle."""

        def __init__(self, registry: typing.Union[provcfg.Registry, persistent.Registry]):
            if isinstance(registry, provcfg.Registry):
                registry = persistent.Registry[registry.reference](**registry.params)
            self._root: root.Level = root.Level(registry)

        def assets(
            self, project: typing.Optional[str], lineage: typing.Optional[str], generation: typing.Optional[str]
        ) -> access.Assets:
            """Create the assets instance of given registry item.

            Args:
                project: Item's project.
                lineage: Item's lineage.
                generation: Item's generation.

            Returns:
                Asset instance.
            """
            return access.Assets(project, lineage, generation, self._root)

        def publish(self, project: str, package: 'distribution.Package') -> None:
            """Publish new package into the registry.

            Args:
                project: Name of project to publish the package into.
                package: Package to be published.
            """
            self._root.get(project).put(package)

        def list(
            self, project: typing.Optional[str], lineage: typing.Optional[str]
        ) -> typing.Iterable['directory.Level.Key']:
            """Repository listing subcommand.

            Args:
                project: Name of project to be listed.
                lineage: Lineage version to be listed.

            Returns:
                Listing of given registry level.
            """
            level = self._root
            if project:
                level = level.get(project)
                if lineage:
                    level = level.get(lineage)
            return level.list()

    class Feeds:
        """Feed pool and util handle."""

        def __init__(self, *configs: typing.Union[provcfg.Feed, 'feedmod.Provider']):
            self._pool: feedmod.Pool = feedmod.Pool(*configs)

        def match(self, query: 'frame.Query') -> 'feedmod.Provider':
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

    def __init__(
        self,
        runner: typing.Optional[typing.Union[provcfg.Runner, str]] = None,
        registry: typing.Optional[typing.Union[provcfg.Registry, persistent.Registry]] = None,
        feeds: typing.Optional[typing.Iterable[typing.Union[provcfg.Feed, str, 'feedmod.Provider']]] = None,
        sink: typing.Optional[typing.Union[provcfg.Sink.Mode, str, sinkmod.Provider]] = None,
    ):
        if isinstance(runner, str):
            runner = provcfg.Runner.resolve(runner)
        self._runner: provcfg.Runner = runner or provcfg.Runner.default
        self._registry: Platform.Registry = self.Registry(registry or provcfg.Registry.default)
        self._feeds: Platform.Feeds = self.Feeds(*(feeds or provcfg.Feed.default))
        self._sink: sinkmod.Handle = sinkmod.Handle(sink or provcfg.Sink.Mode.default)

    def launcher(
        self,
        project: typing.Optional[str],
        lineage: typing.Optional[str] = None,
        generation: typing.Optional[str] = None,
    ) -> 'Platform.Launcher':
        """Get a runner handle for given project/lineage/generation.

        Args:
            project: Project to run.
            lineage: Lineage to run.
            generation: Generation to run.

        Returns:
            Runner handle.
        """
        return self.Launcher(self._runner, self._registry.assets(project, lineage, generation), self._feeds, self._sink)

    @property
    def registry(self) -> 'Platform.Registry':
        """Registry handle getter.

        Returns:
            Registry handle.
        """
        return self._registry

    @property
    def feeds(self) -> 'Platform.Feeds':
        """Feeds handle getter.

        Returns:
            Feeds handle.
        """
        return self._feeds
