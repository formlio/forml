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
import itertools
import shutil
import typing

import click
from click import core

import forml
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.runtime import asset, facility


class Scope(typing.NamedTuple):
    """Case class for holding the partial command config."""

    runner: typing.Optional[str]
    registry: typing.Optional[str]
    feed: typing.Optional[str]
    sink: typing.Optional[str]

    def launcher(
        self, project: str, release: typing.Optional[str], generation: typing.Optional[str]
    ) -> facility.Launcher:
        """Platform launcher helper.

        Args:
            project: Project name.
            release: Release version.
            generation: Generation version.

        Returns:
            Platform launcher.
        """
        return facility.Platform(
            runner=provcfg.Runner.resolve(self.runner),
            registry=provcfg.Registry.resolve(self.registry),
            feeds=provcfg.Feed.resolve(self.feed),
            sink=provcfg.Sink.Mode.resolve(self.sink),
        ).launcher(project, release, generation)

    def scan(self, project: typing.Optional[str], release: typing.Optional[str]) -> typing.Iterable[asset.Level.Key]:
        """Scan the registry level keys.

        Args:
            project: Project to scan.
            release: Release to scan.

        Returns:
            List of level keys.
        """
        return facility.Registry(provcfg.Registry.resolve(self.registry)).list(project, release)


def lprint(listing: typing.Iterable[typing.Any]) -> None:
    """Print list in pretty columns.

    Args:
        listing: Iterable to be printed into columns.
    """
    listing = tuple(str(i) for i in listing)
    if not listing:
        return
    width = max(len(i) for i in listing) + 2
    count = min(shutil.get_terminal_size().columns // width, len(listing))
    for row in itertools.zip_longest(*(listing[i::count] for i in range(count)), fillvalue=''):
        print(*(f'{c:<{width}}' for c in row), sep='')


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
    context.obj = Scope(runner, registry, feed, sink)


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
def scan(scope: Scope, project: typing.Optional[str], release: typing.Optional[str]) -> None:
    """List the content of the selected registry."""
    lprint(scope.scan(project, release))
