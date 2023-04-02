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
import pathlib
import shutil
import sys
import typing

import click
from click import core

import forml

from .. import _conf
from . import application, model, project


class Scope(typing.NamedTuple):
    """Case class for holding the partial command config."""

    config: typing.Optional[pathlib.Path]
    loglevel: typing.Optional[str]
    logfile: typing.Optional[pathlib.Path]

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
@click.option(
    '--config',
    '-C',
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path),
    help='Additional configuration file.',
)
@click.option(
    '--loglevel',
    '-L',
    type=click.Choice(['debug', 'info', 'warning', 'error'], case_sensitive=False),
    help='Global loglevel to use.',
)
@click.option(
    '--logfile',
    type=click.Path(file_okay=True, dir_okay=False, writable=True, path_type=pathlib.Path),
    help='Logfile path.',
)
@click.pass_context
def group(
    context: core.Context,
    config: typing.Optional[pathlib.Path],
    loglevel: typing.Optional[str],
    logfile: typing.Optional[pathlib.Path],
):
    """Life Cycle Management for Data Science Projects."""
    if config:
        _conf.CONFIG.read(config)
    if logfile:
        _conf.CONFIG.update({_conf.SECTION_LOGGING: {_conf.OPT_PATH: logfile}})
    context.obj = Scope(config, loglevel, logfile)


group.add_command(model.group)
group.add_command(project.group)
group.add_command(application.group)


def cli() -> None:
    """Cli wrapper for handling ForML exceptions."""
    try:
        group()  # pylint: disable=no-value-for-parameter
    except forml.AnyError as err:
        print(err, file=sys.stderr)
