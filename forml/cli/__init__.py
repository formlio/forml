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
import abc
import argparse
import itertools
import logging
import shutil
import sys
import typing

from forml import conf, error

LOGGER = logging.getLogger(__name__)

PARSER = argparse.ArgumentParser(add_help=False)
PARSER.add_argument('-C', '--config', type=argparse.FileType(), help='additional config file')
PARSER.add_argument('-L', '--loglevel', help='log with given level')  # not implemented
COMMON = PARSER.parse_known_args()[0]

CLICFG = getattr(COMMON.config, 'name', None)
if CLICFG:
    conf.PARSER.read(CLICFG)


class Handler:
    """Final wrapper of a command implementation and its arguments."""

    def __init__(self, handler: typing.Callable, params: typing.Sequence[str]):
        self._handler: typing.Callable = handler
        self._params: typing.Tuple[str] = tuple(params)

    def __get__(self, _, parser: typing.Type['Parser']):
        def call(namespace: argparse.Namespace) -> None:
            """Parsers bound method created from original handler.

            Args:
                namespace: Parsed arguments.
            """
            params = {a: getattr(namespace, a) for a in self._params}
            LOGGER.debug('Running command with params: %s', params)
            try:
                self._handler(parser, **params)
            except error.Error as err:
                print(err, file=sys.stderr)

        return call


class Builder:
    """Command builder context passed between spec decorators."""

    def __init__(self, handler: typing.Callable):
        self._handler: typing.Callable = handler
        self._params: typing.List['Param'] = list()
        self._command: typing.Optional['Command'] = None

    def add(self, param: 'Param') -> None:
        """Add a parameter to the builder.

        Args:
            param: Parameter to be added.
        """
        self._params.append(param)

    def set(self, command: 'Command') -> None:
        """Set a command to be used by the builder.

        Args:
            command: Command to be used.
        """
        if self._command:
            raise error.Unexpected('Command already provided')
        self._command = command

    def __call__(self, name: str, parser: argparse._SubParsersAction) -> Handler:
        command = parser.add_parser(self._command.name or name, **self._command.kwargs)
        dests = list()
        for param in reversed(self._params):
            dests.append(command.add_argument(*param.args, **param.kwargs).dest)
        return Handler(self._handler, dests)


class Spec(metaclass=abc.ABCMeta):
    """Base class for command decorators."""

    @abc.abstractmethod
    def accept(self, context: 'Builder') -> None:
        """Builder context visitor.

        Args:
            context: Context to visit.
        """

    def __call__(self, inner: typing.Union['Builder', typing.Callable]) -> 'Builder':
        context = inner if isinstance(inner, Builder) else Builder(inner)
        self.accept(context)
        return context


class Param(Spec):
    """Decorator for specifying a parameter."""

    def accept(self, context: 'Builder') -> None:
        context.add(self)

    def __init__(self, *args, **kwargs):
        self.args: typing.Tuple[typing.Any] = args
        self.kwargs: typing.Dict[str, typing.Any] = kwargs


class Command(Spec):
    """Decorator for specifying a command."""

    def __init__(self, name: typing.Optional[str] = None, **kwargs):
        self.name: typing.Optional[str] = name
        self.kwargs: typing.Dict[str, typing.Any] = dict(kwargs)

    def accept(self, context: 'Builder') -> None:
        context.set(self)


class Meta(type):
    """Parser metaclass."""

    CMDKEY = 'command'

    def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any], **kwargs):
        parser = argparse.ArgumentParser(parents=[PARSER], add_help=True, **kwargs)
        subparsers = parser.add_subparsers(
            dest=mcs.CMDKEY, help='program subcommands (-h for individual description)', required=True
        )

        for key, builder in namespace.items():
            if isinstance(builder, Builder):
                namespace[key] = builder(key, subparsers)

        cls = super().__new__(mcs, name, bases, namespace)
        cls.parser = parser
        return cls

    def __call__(cls):
        namespace = cls.parser.parse_args()
        getattr(cls, getattr(namespace, cls.CMDKEY))(namespace)


class Parser(metaclass=Meta):
    """Base class for parsers."""


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
