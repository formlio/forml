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
import functools
import logging
import time
import typing
import uuid

from forml import flow
from forml.runtime import asset

from .. import _exception

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

    def __new__(cls, instruction: Instruction, arguments: typing.Optional[typing.Sequence[Instruction]] = None):
        if arguments is None:
            arguments = []
        if not all(arguments):
            raise _exception.AssemblyError('All arguments required')
        return super().__new__(cls, instruction, tuple(arguments))

    def __repr__(self):
        return f'{self.instruction}{self.arguments}'


class Loader(Instruction):
    """Registry based state loader."""

    def __init__(self, assets: asset.State, key: typing.Union[int, uuid.UUID]):
        self._assets: asset.State = assets
        self._key: typing.Union[int, uuid.UUID] = key

    def execute(self) -> typing.Optional[bytes]:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Returns:
            Loaded state.
        """
        try:
            return self._assets.load(self._key)
        except asset.Level.Listing.Empty:
            LOGGER.warning('No previous generations found - node #%d defaults to no state', self._key)
            return None


class Dumper(Instruction):
    """Registry based state dumper."""

    def __init__(self, assets: asset.State):
        self._assets: asset.State = assets

    def execute(self, state: bytes) -> uuid.UUID:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Args:
            state: State to be persisted.

        Returns:
            Absolute state id.
        """
        return self._assets.dump(state)


class Getter(Instruction):
    """Extracting single item from a vector."""

    def __init__(self, index: int):
        self._index: int = index

    def __repr__(self):
        return super().__repr__() + f'#{self._index}'

    def execute(self, sequence: typing.Sequence[typing.Any]) -> typing.Any:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Args:
            sequence: Sequence of output arguments.

        Returns:
            Single output item.
        """
        return sequence[self._index]


class Committer(Instruction):
    """Commit a new lineage generation."""

    def __init__(self, assets: asset.State):
        self._assets: asset.State = assets

    def execute(self, *states: uuid.UUID) -> None:
        """Instruction functionality.

        Args:
            *states: Sequence of state IDs.
        """
        self._assets.commit(states)


class Action(abc.ABC):
    """Functor action handler."""

    @abc.abstractmethod
    def __call__(self, actor: flow.Actor, *args: typing.Any) -> typing.Any:
        """Action function."""

    def functor(self, spec: flow.Spec) -> 'Functor':
        """Helper method for creating functor instance for this action.

        Args:
            spec: Actor spec instance.

        Returns:
            Functor instance.
        """
        return Functor(spec, self)


ValueT = typing.TypeVar('ValueT')


class Preset(typing.Generic[ValueT], collections.namedtuple('Preset', 'action'), Action, metaclass=abc.ABCMeta):
    """Composite action that presets the actor using the first parameter as value."""

    action: Action
    """Wrapped action."""

    def __call__(self, actor: flow.Actor, *args: typing.Any) -> typing.Any:
        value, *args = args
        LOGGER.debug('Pre-setting actor %s with %d arguments ', actor, len(args))
        if value:
            self.set(actor, value)
        return self.action(actor, *args)

    @abc.abstractmethod
    def set(self, actor: flow.Actor, value: ValueT) -> None:
        """Set operation.

        Args:
            actor: Actor to set the value on.
            value: Value to be set.
        """


class State(Preset[bytes]):
    """State preset action."""

    def set(self, actor: flow.Actor, value: bytes) -> None:
        LOGGER.debug('%s receiving state (%d bytes)', actor, len(value))
        actor.set_state(value)


class Params(Preset[typing.Mapping[str, typing.Any]]):
    """Params preset action."""

    def set(self, actor: flow.Actor, value: typing.Mapping[str, typing.Any]) -> None:
        LOGGER.debug('%s receiving params (%s)', actor, value)
        actor.set_params(**value)


class Mapper(Action):
    """Mapper (transformer) functor action."""

    def __call__(self, actor: flow.Actor, *args) -> typing.Any:
        """Mapper action is the apply method.

        Args:
            actor: Target actor to run the action on.
            *args: List of arguments to be passed to the actor action.

        Returns:
            Output of the apply method.
        """
        result = actor.apply(*args)
        LOGGER.debug('%s result: %.1024s...', actor, result)
        return result


class Trainer(Action):
    """Trainer functor action."""

    def __call__(self, actor: flow.Actor, *args) -> bytes:
        """Trainer action is the train method.

        Args:
            actor: Target actor to run the action on.
            *args: List of arguments to be passed to the actor action.

        Returns:
            New actor state.
        """
        actor.train(*args)
        return actor.get_state()


class Functor(collections.namedtuple('Functor', 'spec, action'), Instruction):
    """Special instruction for wrapping task actors.

    Functor object must be serializable.
    """

    spec: flow.Spec
    action: Action

    def __repr__(self):
        return repr(self.spec)

    def preset_state(self) -> 'Functor':
        """Helper method for returning new functor that prepends the arguments with a state setter."""
        return Functor(self.spec, State(self.action))

    def preset_params(self) -> 'Functor':
        """Helper method for returning new functor that prepends the arguments with a param setter."""
        return Functor(self.spec, Params(self.action))

    @functools.cached_property
    def _actor(self) -> flow.Actor:
        """Cached actor instance.

        Returns:
            Actor instance.
        """
        return self.spec()

    def execute(self, *args) -> typing.Any:
        return self.action(self._actor, *args)
