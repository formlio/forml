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

from forml.conf.parsed import provider as provcfg
from forml.runtime import facility


class Application(typing.NamedTuple):
    """Case class for holding the partial command config."""

    inventory: typing.Optional[str]


@click.group(name='application')
@click.option('-A', '--inventory', type=str, help='Application inventory reference.')
@click.pass_context
def group(
    context: core.Context,
    inventory: typing.Optional[str],
):
    """Application command group."""
    context.obj = Application(inventory)


@group.command()
@click.option('-G', '--gateway', type=str, help='Serving gateway reference.')
@click.option('-M', '--registry', type=str, help='Model registry reference.')
@click.option('-I', '--feed', multiple=True, type=str, help='Input feed references.')
@click.pass_obj
def serve(
    application: Application, gateway: typing.Optional[str], registry: typing.Optional[str], feed: typing.Optional[str]
) -> None:
    """Launch the serving frontend."""
    facility.Platform(
        registry=provcfg.Registry.resolve(registry),
        feeds=provcfg.Feed.resolve(feed),
        inventory=provcfg.Inventory.resolve(application.inventory),
        gateway=provcfg.Gateway.resolve(gateway),
    ).service.run()
