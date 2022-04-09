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
import sys
import typing

import click
from click import core

import forml
from forml import conf

from . import _application, _model, _project


class Scope(typing.NamedTuple):
    """Case class for holding the partial command config."""

    config: typing.Optional[str]
    loglevel: typing.Optional[str]

    @staticmethod
    def print(listing: typing.Iterable[typing.Any]) -> None:
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


@click.group(name='forml')
@click.option('--config', '-C', type=click.Path(exists=True, file_okay=True), help='Additional config file.')
@click.option(
    '--loglevel',
    '-L',
    type=click.Choice(['debug', 'info', 'warning', 'error'], case_sensitive=False),
    help='Global loglevel to use.',
)
@click.pass_context
def group(
    context: core.Context,
    config: typing.Optional[str],
    loglevel: typing.Optional[str],  # pylint: disable=unused-argument
):
    """Lifecycle Management for Datascience Projects."""
    if config:
        conf.PARSER.read(config)
    context.obj = Scope(config, loglevel)


group.add_command(_model.group)
group.add_command(_project.group)
group.add_command(_application.group)


def main() -> None:
    """Cli wrapper for handling ForML exceptions."""
    try:
        group()  # pylint: disable=no-value-for-parameter
    except forml.AnyError as err:
        print(err, file=sys.stderr)
