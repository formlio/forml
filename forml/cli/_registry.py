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

from forml.conf.parsed import provider as provcfg
from forml.runtime import asset, facility


class Registry(typing.NamedTuple):
    """Case class for holding the partial command config."""

    provider: typing.Optional[str]

    def scan(self, project: typing.Optional[str], release: typing.Optional[str]) -> typing.Iterable[asset.Level.Key]:
        """Scan the registry level keys.

        Args:
            project: Project to scan.
            release: Release to scan.

        Returns:
            List of level keys.
        """
        return facility.Registry(provcfg.Registry.resolve(self.provider)).list(project, release)


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


@click.group(name='registry')
@click.option('-P', '--provider', type=str, help='Persistent registry reference.')
@click.pass_context
def group(context: core.Context, provider: typing.Optional[str]):
    """Model registry command group."""
    context.obj = Registry(provider)


@group.command(name='list')
@click.argument('project', required=False)
@click.argument('release', required=False)
@click.pass_obj
def scan(registry: Registry, project: typing.Optional[str], release: typing.Optional[str]) -> None:
    """List the content of the selected registry."""
    lprint(registry.scan(project, release))
