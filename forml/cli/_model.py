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
ForML command line interface.
"""
import collections
import typing

import click
from click import core

import forml
from forml import runtime
from forml.conf.parsed import provider as provcfg
from forml.io import asset, dsl

if typing.TYPE_CHECKING:
    from forml import cli


class Scope(collections.namedtuple('Scope', 'parent, runner, registry, feeds, sink')):
    """Case class for holding the partial command config."""

    parent: 'cli.Scope'
    runner: provcfg.Runner
    registry: provcfg.Registry
    feeds: tuple[provcfg.Feed]
    sink: provcfg.Sink

    def __new__(
        cls,
        parent: 'cli.Scope',
        runner: typing.Optional[str],
        registry: typing.Optional[str],
        feed: typing.Optional[str],
        sink: typing.Optional[str],
    ):
        return super().__new__(
            cls,
            parent,
            provcfg.Runner.resolve(runner),
            provcfg.Registry.resolve(registry),
            tuple(provcfg.Feed.resolve(feed)),
            provcfg.Sink.Mode.resolve(sink),
        )

    def launcher(
        self, project: str, release: typing.Optional[str], generation: typing.Optional[str]
    ) -> runtime.Launcher:
        """Platform launcher helper.

        Args:
            project: Project name.
            release: Release version.
            generation: Generation version.

        Returns:
            Platform launcher.
        """
        return runtime.Platform(runner=self.runner, registry=self.registry, feeds=self.feeds, sink=self.sink).launcher(
            project, release, generation
        )

    def scan(self, project: typing.Optional[str], release: typing.Optional[str]) -> typing.Iterable[asset.Level.Key]:
        """Scan the registry level keys.

        Args:
            project: Project to scan.
            release: Release to scan.

        Returns:
            List of level keys.
        """
        return runtime.Repo(self.registry).list(project, release)


@click.group(name='model')
@click.option('-R', '--runner', type=str, help='Runtime runner reference.')
@click.option('-M', '--registry', type=str, help='Model registry reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.option('-O', '--sink', type=str, help='Output sink reference.')
@click.pass_context
def group(
    context: core.Context,
    runner: typing.Optional[str],
    registry: typing.Optional[str],
    feed: typing.Optional[str],
    sink: typing.Optional[str],
):
    """Model command group."""
    context.obj = Scope(context.obj, runner, registry, feed, sink)


@group.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def tune(
    model: Scope,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Tune new generation of the given (or default) project release."""
    raise forml.MissingError(f'Tuning project {project}... not implemented')


@group.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def train(
    scope: Scope,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Train new generation of the given (or default) project release."""
    scope.launcher(project, release, generation).train(lower, upper)


@group.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def apply(
    scope: Scope,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Apply the given (or default) generation."""
    scope.launcher(project, release, generation).apply(lower, upper)


@group.command(name='eval')
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def evaluate(
    scope: Scope,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Evaluate predictions of the given (or default) generation."""
    scope.launcher(project, release, generation).apply_eval(lower, upper)


@group.command(name='list')
@click.argument('project', required=False)
@click.argument('release', required=False)
@click.pass_obj
def list_(scope: Scope, project: typing.Optional[str], release: typing.Optional[str]) -> None:
    """List the content of the selected registry."""
    scope.parent.print(scope.scan(project, release))
