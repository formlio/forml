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
Actor related actions/instruction.
"""
import abc
import collections
import inspect
import logging
import typing

from .. import target

if typing.TYPE_CHECKING:
    from forml import flow


LOGGER = logging.getLogger(__name__)


class Action(abc.ABC):
    """Functor action handler."""

    @abc.abstractmethod
    def __call__(self, actor: 'flow.Actor', *args: typing.Any) -> typing.Any:
        """Action function."""

    def __repr__(self):
        return self.__class__.__name__.lower()

    def functor(self, builder: 'flow.Builder') -> 'Functor':
        """Helper method for creating functor instance for this action.

        Args:
            builder: Actor builder instance.

        Returns:
            Functor instance.
        """
        return Functor(builder, self)

    def reduce(
        self, actor: 'flow.Actor', *args: typing.Any  # pylint: disable=unused-argument
    ) -> tuple['Action', typing.Sequence[typing.Any]]:
        """Reduce the actions on the given actor to a discrete action and its direct parameters.

        Args:
            actor: Actor subject.
            *args: Args sequence with the presets in front.

        Returns:
            Discrete action and its direct parameters.
        """
        return self, args

    def __contains__(self, action: type['Action']) -> bool:
        """Check whether the given action type is performed in our scope.

        Args:
            action: Type of action to be searched for.

        Returns:
            True if our action involves the given action type.
        """
        return inspect.isclass(action) and isinstance(self, action)


Value = typing.TypeVar('Value')


class Preset(typing.Generic[Value], Action, metaclass=abc.ABCMeta):
    """Composite action that presets the actor using the first parameter as value."""

    def __init__(self, action: Action):
        self._action: Action = action

    def __repr__(self):
        return f'{self.__class__.__name__.lower()}.{self._action}'

    def __call__(self, actor: 'flow.Actor', *args: typing.Any) -> typing.Any:
        action, args = self.reduce(actor, *args)
        return action(actor, *args)

    def reduce(self, actor: 'flow.Actor', *args: typing.Any) -> tuple[Action, typing.Sequence[typing.Any]]:
        value, *args = args
        LOGGER.debug('Pre-setting actor %s with %d arguments ', actor, len(args))
        if value:
            self.set(actor, value)
        return self._action.reduce(actor, *args)

    def __contains__(self, action: type[Action]) -> bool:
        return super().__contains__(action) or self._action.__contains__(action)

    @abc.abstractmethod
    def set(self, actor: 'flow.Actor', value: Value) -> None:
        """Set operation.

        Args:
            actor: Actor to set the value on.
            value: Value to be set.
        """


class SetState(Preset[bytes]):
    """State preset action."""

    def set(self, actor: 'flow.Actor', value: bytes) -> None:
        LOGGER.debug('%s receiving state (%d bytes)', actor, len(value))
        params = actor.get_params()
        actor.set_state(value)
        actor.set_params(**params)


class SetParams(Preset[typing.Mapping[str, typing.Any]]):
    """Params preset action."""

    def set(self, actor: 'flow.Actor', value: typing.Mapping[str, typing.Any]) -> None:
        LOGGER.debug('%s receiving params (%s)', actor, value)
        actor.set_params(**value)


class Apply(Action):
    """Mapper (transformer) functor action."""

    def __call__(self, actor: 'flow.Actor', *args) -> typing.Any:
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


class Train(Action):
    """Trainer functor action."""

    def __call__(self, actor: 'flow.Actor', *args) -> bytes:
        """Trainer action is the train method.

        Args:
            actor: Target actor to run the action on.
            *args: List of arguments to be passed to the actor action.

        Returns:
            New actor state.
        """
        actor.train(*args)
        return actor.get_state()


class Functor(collections.namedtuple('Functor', 'builder, action'), target.Instruction):
    """Special instruction for wrapping task actors.

    Functor object must be serializable.
    """

    builder: 'flow.Builder'
    action: Action

    def __repr__(self):
        return f'{self.builder}.{self.action}'

    def __hash__(self):
        return id(self)

    def preset_state(self) -> 'Functor':
        """Helper method for returning new functor that prepends the arguments with a state setter."""
        return Functor(self.builder, SetState(self.action))

    def preset_params(self) -> 'Functor':
        """Helper method for returning new functor that prepends the arguments with a param setter."""
        return Functor(self.builder, SetParams(self.action))

    def execute(self, *args) -> typing.Any:
        return self.action(self.builder(), *args)
