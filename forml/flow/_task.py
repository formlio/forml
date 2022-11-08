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
import inspect
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
    """Abstract actor base class.

    This is a generic class with parametric input types ``flow.Features``, ``flow.Labels`` and
    output type ``flow.Result``.
    """

    def __repr__(self):
        return name(self.__class__, **self.get_params())

    @abc.abstractmethod
    def apply(self, *features: 'flow.Features') -> 'flow.Result':
        """The *apply* mode entry-point.

        Mandatory method engaging the M:N input-output *Apply* ports.

        Args:
            features: Input feature-set(s).

        Returns:
            Transformation result (i.e. predictions).
        """

    def train(self, features: 'flow.Features', labels: 'flow.Labels', /) -> None:
        """The *train* mode entry point.

        Optional method engaging the *Train* (``features``) and *Label* (``labels``) ports of
        stateful actors.

        Unlike with the multiple apply-mode feature ports, there can only be a single train-mode
        feature port.

        Args:
            features: Train feature-set.
            labels: Train labels.
        """
        raise RuntimeError('Stateless actor')

    def get_state(self) -> bytes:
        """Return the internal state of the actor.

        The *State* output port representation.

        The particular bytes-encoding of the returned value can be arbitrary as long as it is
        acceptable by the companion :meth:`set_state` method.

        The default implementation is using :doc:`Python Pickle <python:library/pickle>` for
        serializing the entire actor object.

        Returns:
            State as bytes.
        """
        if not self.is_stateful():
            return b''
        LOGGER.debug('Getting %s state', self)
        return cloudpickle.dumps(self.__dict__)

    def set_state(self, state: bytes) -> None:
        """Set the new internal state of the actor.

        The *State* input port representation.

        The default implementation is interpreting the state as the entire actor object serialized
        by :doc:`Python Pickle <python:library/pickle>`.

        Args:
            state: Bytes to be used as internal state.
        """
        if not state:
            return
        if not self.is_stateful():
            raise forml.UnexpectedError('State provided but actor stateless')
        LOGGER.debug('Setting %s state (%d bytes)', self, len(state))
        params = self.get_params()  # keep the original hyper-params
        self.__dict__.update(cloudpickle.loads(state))
        self.set_params(**params)  # restore the original hyper-params

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        """Get the current hyper-parameters of the actor.

        The *Params* output port representation.

        All the values returned by this method must be acceptable by the companion
        :meth:`set_params`.

        The default implementation returns empty mapping.

        Returns:
            Dictionary of the name-value of the hyper-parameters.
        """
        return {}

    def set_params(self, **params: typing.Any) -> None:
        """Set new hyper-parameters of the actor (typically by a hyper-parameter tuner).

        The *Params* input port representation.

        The implementation of this method can choose to accept only a subset of the constructor
        arguments if some of them are not expected to be changed during the lifetime.

        Args:
            params: New hyper-parameters as keyword arguments.
        """
        if params:
            raise NotImplementedError(f'Params setter for {params} not implemented on {self}')

    @classmethod
    def builder(cls: 'type[_Actor]', *args, **kwargs: typing.Any) -> 'flow.Builder[_Actor]':
        """Creating a builder instance for this actor.

        Args:
            args: Positional arguments.
            kwargs: Keyword arguments.

        Returns:
            Actor builder instance.
        """
        return Spec(cls, *args, **kwargs)

    @classmethod
    def is_stateful(cls) -> bool:
        """Check whether this actor is stateful.

        By default, this is determined based on the existence of user-overridden train method.

        Returns:
            True if stateful.
        """
        return cls.train.__code__ is not Actor.train.__code__


# Generic actor type.
_Actor = typing.TypeVar('_Actor', bound=Actor)


class Builder(typing.Generic[_Actor], metaclass=abc.ABCMeta):
    """Interface for actor builders providing all the required initialization configuration
    for instantiating an actor."""

    @property
    @abc.abstractmethod
    def actor(self) -> type[_Actor]:
        """Target actor class."""

    @property
    @abc.abstractmethod
    def args(self) -> typing.Sequence[typing.Any]:
        """Actor positional arguments."""

    @property
    @abc.abstractmethod
    def kwargs(self) -> typing.Mapping[str, typing.Any]:
        """Actor keyword arguments."""

    def update(self, *args, **kwargs) -> 'flow.Builder[_Actor]':
        """Return new builder with the updated parameters.

        Args:
            args: Positional arguments to *replace* the original ones.
            kwargs: Keyword arguments to *update* the original ones.

        Returns:
            New builder instance with the updated parameters.
        """
        return self.actor.builder(*(args or self.args), **self.kwargs | kwargs)

    def reset(self, *args, **kwargs) -> 'flow.Builder[_Actor]':
        """Return new builder with the new parameters.

        Args:
            args: Positional arguments to *replace* the original ones.
            kwargs: Keyword arguments to *replace* the original ones.

        Returns:
            New builder instance with the new parameters.
        """
        return self.actor.builder(*args, **kwargs)

    def __repr__(self):
        return name(self.actor, *self.args, **self.kwargs)

    def __call__(self, *args, **kwargs) -> _Actor:
        return self.actor(*(args or self.args), **self.kwargs | kwargs)


@typing.final
class Spec(collections.namedtuple('Spec', 'actor, args, kwargs'), Builder[_Actor]):
    """Actor builder holding all the required initialization configuration for instantiating the
    particular actor.

    Args:
        actor: Target actor class.
        args: Actor positional arguments.
        kwargs: Actor keyword arguments.
    """

    actor: type[_Actor]
    args: tuple[typing.Any]
    kwargs: typing.Mapping[str, typing.Any]

    def __new__(cls, actor: type[_Actor], *args: typing.Any, **kwargs: typing.Any):
        inspect.signature(actor).bind_partial(*args, **kwargs)
        return super().__new__(cls, actor, args, types.MappingProxyType(kwargs))

    def __getnewargs_ex__(self):
        return (self.actor, *self.args), dict(self.kwargs)

    def __repr__(self) -> str:
        return Builder.__repr__(self)
