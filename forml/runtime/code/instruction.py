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
import logging
import typing
import uuid

from forml.flow import task
from forml.runtime import code
from forml.runtime.asset import access, directory

LOGGER = logging.getLogger(__name__)


class Loader(code.Instruction):
    """Registry based state loader."""

    def __init__(self, assets: access.State, key: typing.Union[int, uuid.UUID]):
        self._assets: access.State = assets
        self._key: typing.Union[int, uuid.UUID] = key

    def execute(self) -> typing.Optional[bytes]:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Returns:
            Loaded state.
        """
        try:
            return self._assets.load(self._key)
        except directory.Level.Listing.Empty:
            LOGGER.warning('No previous generations found - node #%d defaults to no state', self._key)
            return None


class Dumper(code.Instruction):
    """Registry based state dumper."""

    def __init__(self, assets: access.State):
        self._assets: access.State = assets

    def execute(self, state: bytes) -> uuid.UUID:  # pylint: disable=arguments-differ
        """Instruction functionality.

        Args:
            state: State to be persisted.

        Returns:
            Absolute state id.
        """
        return self._assets.dump(state)


class Getter(code.Instruction):
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


class Committer(code.Instruction):
    """Commit a new lineage generation."""

    def __init__(self, assets: access.State):
        self._assets: access.State = assets

    def execute(self, *states: uuid.UUID) -> None:
        """Instruction functionality.

        Args:
            *states: Sequence of state IDs.
        """
        self._assets.commit(states)


class Functor(code.Instruction):
    """Special instruction for wrapping task actors.

    Functor object must be serializable.
    """

    class Shifting:
        """Extra functionality to be prepended to the main objective."""

        def __init__(
            self,
            consumer: typing.Callable[[task.Actor, typing.Any], None],
            objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any],
        ):
            self._consumer: typing.Callable[[task.Actor, typing.Any], None] = consumer
            self._objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any] = objective

        def __call__(self, actor: task.Actor, first: typing.Any, *args: typing.Any) -> typing.Any:
            LOGGER.debug('Shifting functor %s left with %d arguments ', actor, len(args))
            if first:
                self._consumer(actor, first)
            return self._objective(actor, *args)

        @staticmethod
        def state(actor: task.Actor, state: bytes) -> None:
            """Predefined shifting for state taking objective.

            Args:
                actor: Target actor to run the objective on.
                state: Actor state to be used.

            Returns:
                Actor instance.
            """
            LOGGER.debug('%s receiving state (%d bytes)', actor, len(state))
            actor.set_state(state)

        @staticmethod
        def params(actor: task.Actor, params: typing.Mapping[str, typing.Any]) -> None:
            """Predefined shifting for params taking objective.

            Args:
                actor: Target actor to run the objective on.
                params: Actor params to be used.

            Returns:
                Actor instance.
            """
            LOGGER.debug('%s receiving params (%s)', actor, params)
            actor.set_params(**params)

    def __init__(
        self, spec: task.Spec, objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any]
    ):
        self._spec: task.Spec = spec
        self._objective: typing.Callable[[task.Actor, typing.Sequence[typing.Any]], typing.Any] = objective
        self._instance: typing.Optional[task.Actor] = None  # transient

    def __reduce__(self):
        return Functor, (self._spec, self._objective)

    def __repr__(self):
        return repr(self._spec)

    def shiftby(self, consumer: typing.Callable[[task.Actor, typing.Any], None]) -> 'Functor':
        """Create new functor with its objective prepended by an extra consumer.

        Args:
            consumer: Callable taking the target actor and eating its first argument.

        Returns:
            New Functor instance with the objective updated.
        """
        return Functor(self._spec, self.Shifting(consumer, self._objective))

    @property
    def _actor(self) -> task.Actor:
        """Internal cached actor instance.

        Returns:
            Actor instance.
        """
        if not self._instance:
            self._instance = self._spec()
        return self._instance

    def execute(self, *args) -> typing.Any:
        return self._objective(self._actor, *args)


class Functional(Functor, metaclass=abc.ABCMeta):
    """Base class for mapper and consumer functors."""

    def __init__(self, spec: task.Spec):
        super().__init__(spec, self._function)

    @staticmethod
    @abc.abstractmethod
    def _function(actor: task.Actor, *args) -> typing.Any:
        """Delegated actor objective.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns:
            Anything the actor objective returns.
        """


class Mapper(Functional):
    """Mapper (transformer) functor."""

    @staticmethod
    def _function(actor: task.Actor, *args) -> typing.Any:
        """Mapper objective is the apply method.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns:
            Output of the apply method.
        """
        result = actor.apply(*args)
        LOGGER.debug('%s result: %.1024s...', actor, result)
        return result


class Consumer(Functional):
    """Consumer (ie trainer) functor."""

    @staticmethod
    def _function(actor: task.Actor, *args) -> bytes:
        """Consumer objective is the train method.

        Args:
            actor: Target actor to run the objective on.
            *args: List of arguments to be passed to the actor objective.

        Returns:
            New actor state.
        """
        actor.train(*args)
        return actor.get_state()
