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
import pathlib
import typing

import click
from click import core
from setuptools import sandbox

import forml
from forml.io import dsl

if typing.TYPE_CHECKING:
    from forml import cli


class Scope(collections.namedtuple('Scope', 'parent, path')):
    """Case class for holding the partial command config."""

    parent: 'cli.Scope'
    path: pathlib.Path

    def __new__(cls, parent: 'cli.Scope', path: typing.Optional[str]):
        return super().__new__(cls, parent, pathlib.Path(path or '.'))

    def run_setup(self, *argv):
        """Interim hack to call the setup.py"""
        sandbox.run_setup(self.path / 'setup.py', argv)


@click.group(name='project')
@click.option('--path', type=click.Path(exists=True, dir_okay=True), help='Project root directory.')
@click.pass_context
def group(context: core.Context, path: typing.Optional[str]):
    """Project command group."""
    context.obj = Scope(context.obj, path)


@group.command()
@click.argument('name', required=True)
@click.option('--package', type=str, help='Full python package path to be used.')
@click.option('-r', '--requirements', multiple=True, type=str, help='List of install requirements.')
@click.pass_obj
def init(
    scope: Scope, name: str, package: typing.Optional[str], requirements: typing.Optional[typing.Sequence[str]]
) -> None:
    """Create skeleton for a new project."""
    raise forml.MissingError(f'Creating project {name}... not implemented')


@group.command()
@click.pass_obj
def test(scope: Scope) -> None:
    """Run the unit tests."""
    scope.run_setup('test')


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
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Train the project."""
    scope.run_setup('train')


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
    lower: typing.Optional[dsl.Native],
    upper: typing.Optional[dsl.Native],
) -> None:
    """Evaluate the project."""
    scope.run_setup('eval')


@group.command()
@click.pass_obj
def release(scope: Scope) -> None:
    """Run the unit tests."""
    scope.run_setup('bdist_4ml', 'upload')
