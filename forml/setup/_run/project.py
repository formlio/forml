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
import re
import typing

import click
from click import core

import forml
from forml import project

from .. import _templating

if typing.TYPE_CHECKING:
    from forml.io import dsl

    from .. import _run


class Scope(collections.namedtuple('Scope', 'parent, tree')):
    """Case class for holding the partial command config."""

    parent: '_run.Scope'
    tree: project.Tree

    def __new__(cls, parent: '_run.Scope', path: typing.Optional[str]):
        return super().__new__(cls, parent, project.Tree(path or '.'))

    def run(self, *argv: str, **options) -> None:
        """Call the project manager."""
        self.tree.run(*argv, **options)

    def create_project(
        self,
        name: str,
        template: typing.Optional[str],
        package: typing.Optional[str],
        version: typing.Optional[str],
        requirements: typing.Sequence[str],
    ) -> None:
        """Helper for creating a new project structure."""
        _templating.project(name, self.tree.path, template, package, version, requirements)


@click.group(name='project')
@click.option('--path', type=click.Path(exists=False, dir_okay=True, file_okay=False), help='Project root directory.')
@click.pass_context
def group(context: core.Context, path: typing.Optional[str]):
    """Project command group (development life cycle)."""
    context.obj = Scope(context.obj, path)


@group.command()
@click.argument('name', required=True)
@click.option('--template', type=str, help='Name of existing project template.')
@click.option('--package', type=str, help='Full python package path to be used.')
@click.option('--version', type=str, help='Initial project version.')
@click.option('-r', '--requirements', multiple=True, type=str, help='List of install requirements.')
@click.pass_obj
def init(
    scope: Scope,
    name: str,
    template: typing.Optional[str],
    package: typing.Optional[str],
    version: typing.Optional[str],
    requirements: typing.Sequence[str],
) -> None:
    """Create skeleton for a new project."""
    deps = {r.strip() for t in requirements for r in t.split(',')}
    if not any(re.match(rf'^{forml.__name__}\W', d) for d in deps):
        deps.add(f'{forml.__name__}=={forml.__version__}')
    scope.create_project(name, template, package, version, sorted(deps))


@group.command()
@click.pass_obj
def test(scope: Scope) -> None:
    """Run the unit tests."""
    scope.run('test')


@group.command()
@click.option('-R', '--runner', type=str, help='Runtime runner reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def train(
    scope: Scope,
    runner: typing.Optional[str],
    feed: typing.Optional[typing.Sequence[str]],
    lower: typing.Optional['dsl.Native'],
    upper: typing.Optional['dsl.Native'],
) -> None:
    """Train the project."""
    scope.run('train', runner=runner, feed=','.join(feed), lower=lower, upper=upper)


@group.command(name='eval')
@click.option('-R', '--runner', type=str, help='Runtime runner reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.option('--lower', help='Dataset lower ordinal.')
@click.option('--upper', help='Dataset upper ordinal.')
@click.pass_obj
def evaluate(
    scope: Scope,
    runner: typing.Optional[str],
    feed: typing.Optional[typing.Sequence[str]],
    lower: typing.Optional['dsl.Native'],
    upper: typing.Optional['dsl.Native'],
) -> None:
    """Evaluate the project."""
    scope.run('eval', runner=runner, feed=','.join(feed), lower=lower, upper=upper)


@group.command()
@click.pass_obj
def release(scope: Scope) -> None:
    """Run the unit tests."""
    scope.run('bdist_4ml', 'upload')
