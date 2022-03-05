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
import typing

import click
from click import core

import forml
from forml.conf.parsed import provider as provcfg
from forml.io import dsl
from forml.runtime import facility


class Model(typing.NamedTuple):
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
            provcfg.Runner.resolve(self.runner),
            provcfg.Registry.resolve(self.registry),
            provcfg.Feed.resolve(self.feed),
            provcfg.Sink.Mode.resolve(self.sink),
        ).launcher(project, release, generation)


@click.group(name='model')
@click.option('-R', '--runner', type=str, help='Runtime runner reference.')
@click.option('-P', '--registry', type=str, help='Persistent registry reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.option('-O', '--sink', type=str, help='Output sink reference.')
@click.pass_context
def main(
    context: core.Context,
    runner: typing.Optional[str],
    registry: typing.Optional[str],
    feed: typing.Optional[str],
    sink: typing.Optional[str],
):
    """Model command group."""
    context.obj = Model(runner, registry, feed, sink)


@main.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def tune(
    model: Model,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Tune new generation of the given (or default) project release."""
    raise forml.MissingError(f'Tuning project {project}... not implemented')


@main.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def train(
    model: Model,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Train new generation of the given (or default) project release."""
    model.launcher(project, release, generation).train(lower, upper)


@main.command()
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def apply(
    model: Model,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Apply the given (or default) generation."""
    model.launcher(project, release, generation).apply(lower, upper)


@main.command(name='eval')
@click.argument('project', required=True)
@click.argument('release', required=False)
@click.argument('generation', required=False)
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def evaluate(
    model: Model,
    project: str,
    release: typing.Optional[str],
    generation: typing.Optional[str],
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Evaluate predictions of the given (or default) generation."""
    model.launcher(project, release, generation).apply_eval(lower, upper)
