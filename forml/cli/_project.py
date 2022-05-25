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

if typing.TYPE_CHECKING:
    from forml import cli


class Scope(typing.NamedTuple):
    """Case class for holding the partial command config."""

    parent: 'cli.Scope'
    path: typing.Optional[str]


@click.group(name='project')
@click.option('--path', type=click.Path(exists=True, dir_okay=True), help='Project root directory.')
@click.pass_context
def group(context: core.Context, path: typing.Optional[str]):
    """Project command group."""
    context.obj = Scope(context.obj, path)


@group.command()
@click.argument('name', required=True)
@click.option('--package', type=str, help='Full python package path to be used.')
@click.pass_obj
def init(scope: Scope, name: str, package: typing.Optional[str]) -> None:
    """Create skeleton for a new project."""
    raise forml.MissingError(f'Creating project {name}... not implemented')
