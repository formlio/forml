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

from forml import project, runtime
from forml.conf.parsed import provider as provcfg
from forml.io import asset

if typing.TYPE_CHECKING:
    from forml import cli


class Scope(collections.namedtuple('Scope', 'parent, inventory')):
    """Case class for holding the partial command config."""

    parent: 'cli.Scope'
    inventory: provcfg.Inventory

    def __new__(cls, parent: 'cli.Scope', inventory: typing.Optional[str]):
        return super().__new__(cls, parent, provcfg.Inventory.resolve(inventory))

    @property
    def descriptors(self) -> asset.Inventory:
        """Get the descriptor inventory instance.

        Returns:
            Inventory instance.
        """
        return asset.Inventory[self.inventory.reference](**self.inventory.params)


@click.group(name='application')
@click.option('-A', '--inventory', type=str, help='Application inventory reference.')
@click.pass_context
def group(
    context: core.Context,
    inventory: typing.Optional[str],
):
    """Application command group."""
    context.obj = Scope(context.obj, inventory)


@group.command()
@click.argument('descriptor', type=click.Path(exists=True, file_okay=True))
@click.pass_obj
def put(scope: Scope, descriptor: str) -> None:
    """Store the application descriptor into the inventory."""
    handle = project.Descriptor.Handle(descriptor)
    scope.descriptors.put(handle)


@group.command()
@click.option('-G', '--gateway', type=str, help='Serving gateway reference.')
@click.option('-M', '--registry', type=str, help='Model registry reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.pass_obj
def serve(
    scope: Scope, gateway: typing.Optional[str], registry: typing.Optional[str], feed: typing.Optional[str]
) -> None:
    """Launch the serving frontend."""
    runtime.Platform(
        registry=provcfg.Registry.resolve(registry),
        feeds=provcfg.Feed.resolve(feed),
        inventory=scope.inventory,
        gateway=provcfg.Gateway.resolve(gateway),
    ).service.run()


@group.command(name='list')
@click.pass_obj
def list_(scope: Scope) -> None:
    """List the application descriptors in the selected inventory."""
    scope.parent.print(scope.descriptors.list())
