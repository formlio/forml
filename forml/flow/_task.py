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
Flow actor abstraction.
"""

import abc
import collections
import logging
import types
import typing

import cloudpickle

import forml

if typing.TYPE_CHECKING:
    from forml import flow


LOGGER = logging.getLogger(__name__)


def name(actor: typing.Any, *args, **kwargs) -> str:
    """Infer the task name of given instance or type.

    Args:
        actor: Type or actor instance.
        *args: Optional positional parameters.
        **kwargs: Optional keyword parameters.

    Returns:
        String name representation.
    """

    def extract(obj: typing.Any) -> str:
        """Extract the name of given object
        Args:
            obj: Object whose name to be extracted.

        Returns:
            Extracted name.
        """
        return obj.__name__ if hasattr(obj, '__name__') else repr(obj)

    value = extract(actor)
    params = [extract(a) for a in args] + [f'{k}={extract(v)}' for k, v in kwargs.items()]
    if params:
        value += '(' + ', '.join(params) + ')'
    return value


# Actor features type.
Features = typing.TypeVar('Features')

# Actor labels type.
Labels = typing.TypeVar('Labels')

# Actor apply result type.
Result = typing.TypeVar('Result')


class Actor(typing.Generic[Features, Labels, Result], metaclass=abc.ABCMeta):
    """Abstract interface of an actor."""

    @classmethod
    def spec(cls: 'type[_Actor]', *args, **kwargs: typing.Any) -> 'Spec[_Actor]':
        """Shortcut for creating a spec of this actor.

        Args:
            *args: Positional params.
            **kwargs: Keyword params.

        Returns:
            Actor spec instance.
        """
        return Spec(cls, *args, **kwargs)

    @classmethod
    def is_stateful(cls) -> bool:
        """Check whether this actor is stateful (by default determined based on existence of user-overridden train
        method).

        Returns:
            True if stateful.
        """
        return cls.train.__code__ is not Actor.train.__code__

    def train(self, features: 'flow.Features', labels: 'flow.Labels', /) -> None:  # pylint: disable=no-self-use
        """Train the actor using the provided features and label.

        Optional method engaging the *Train* (``features``) and *Label* (``label``) ports on stateful actors.

        Note the stateful Actor can only have single-port features input.

        Args:
            features: Table of feature vectors.
            labels: Table of labels.
        """
        raise RuntimeError('Stateless actor')

    @abc.abstractmethod
    def apply(self, *features: 'flow.Features') -> 'flow.Result':
        """Pass features through the apply function (typically transform or predict).

        Mandatory M:N input-output *Apply* ports in case of stateless Actor or 1:N in case of a stateful one.

        Args:
            features: Table(s) of feature vectors.

        Returns:
            Transformed features (ie predictions).
        """

    def get_params(self) -> typing.Mapping[str, typing.Any]:  # pylint: disable=no-self-use
        """Get hyper-parameters of this actor.

        Mandatory input and output *Params* ports.

        Returns:
            Dictionary of the name-value of the hyperparameters. All of the returned parameters must be acceptable
            by the companion set_params.
        """
        return {}

    def set_params(self, **params: typing.Any) -> None:
        """Set hyper-parameters of this actor.

        Args:
            params: Dictionary of hyper parameters.
        """
        if params:
            raise RuntimeError(f'Params setter for {params} not implemented on {self}')

    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        Returns:
            State as bytes.
        """
        if not self.is_stateful():
            return bytes()
        LOGGER.debug('Getting %s state', self)
        return cloudpickle.dumps(self.__dict__)

    def set_state(self, state: bytes) -> None:
        """Set new internal state of the actor. Note this doesn't change the setting of the actor hyper-parameters.

        Args:
            state: bytes to be used as internal state.
        """
        if not state:
            return
        if not self.is_stateful():
            raise forml.UnexpectedError('State provided but actor stateless')
        LOGGER.debug('Setting %s state (%d bytes)', self, len(state))
        params = self.get_params()  # keep the original hyper-params
        self.__dict__.update(cloudpickle.loads(state))
        self.set_params(**params)  # restore the original hyper-params

    def __repr__(self):
        return name(self.__class__, **self.get_params())


# Generic actor type.
_Actor = typing.TypeVar('_Actor', bound=Actor)


class Spec(typing.Generic[_Actor], collections.namedtuple('Spec', 'actor, args, kwargs')):
    """Wrapper of actor class and init params."""

    actor: type[_Actor]
    args: tuple[typing.Any]
    kwargs: typing.Mapping[str, typing.Any]

    def __new__(cls, actor: type[_Actor], *args: typing.Any, **kwargs: typing.Any):
        return super().__new__(cls, actor, args, types.MappingProxyType(kwargs))

    def __repr__(self):
        return name(self.actor, *self.args, **self.kwargs)

    def __getnewargs_ex__(self):
        return (self.actor, *self.args), dict(self.kwargs)

    def __call__(self, *args, **kwargs) -> _Actor:
        return self.actor(*(args or self.args), **self.kwargs | kwargs)
