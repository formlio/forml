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
Code instructions.
"""
import abc
import collections
import logging
import time
import typing

from ... import _exception

LOGGER = logging.getLogger(__name__)


class Instruction(metaclass=abc.ABCMeta):
    """Callable part of an assembly symbol that's responsible for implementing the processing activity."""

    @abc.abstractmethod
    def execute(self, *args: typing.Any) -> typing.Any:
        """Instruction functionality.

        Args:
            *args: Sequence of input arguments.

        Returns:
            Instruction result.
        """

    def __repr__(self):
        return self.__class__.__name__

    def __call__(self, *args: typing.Any) -> typing.Any:
        LOGGER.debug('%s invoked (%d args)', self, len(args))
        start = time.time()
        try:
            result = self.execute(*args)
        except Exception as err:
            LOGGER.exception(
                'Instruction %s failed when processing arguments: %s', self, ', '.join(f'{str(a):.1024s}' for a in args)
            )
            raise err
        LOGGER.debug('%s completed (%.2fms)', self, (time.time() - start) * 1000)
        return result


class Symbol(collections.namedtuple('Symbol', 'instruction, arguments')):
    """Main entity of the assembled code."""

    instruction: Instruction
    arguments: tuple[Instruction]

    def __new__(cls, instruction: Instruction, arguments: typing.Optional[typing.Sequence[Instruction]] = None):
        if arguments is None:
            arguments = []
        if not all(arguments):
            raise _exception.AssemblyError('All arguments required')
        return super().__new__(cls, instruction, tuple(arguments))

    def __repr__(self):
        return f'{self.instruction}{self.arguments}'
