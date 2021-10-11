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
Utilities for creating actors using decorator wrappings.
"""

import abc
import inspect
import itertools
import typing

import forml
from forml import flow

Target = typing.Union[str, typing.Callable[..., typing.Any]]
ValueT = typing.TypeVar('ValueT')
ActorT = typing.TypeVar('ActorT')


class Proxy(typing.Generic[ActorT, ValueT], metaclass=abc.ABCMeta):
    """Base class for wrappers."""

    def __init__(self, actor: ActorT, params: typing.Mapping[str, ValueT]):
        self._actor: ActorT = actor
        self._params: typing.Mapping[str, ValueT] = params

    def __hash__(self):
        return hash(self._actor) ^ hash(tuple(sorted(self._params.items())))

    def __eq__(self, other: typing.Any):
        # pylint: disable=protected-access
        return isinstance(other, self.__class__) and self._actor == other._actor and self._params == other._params

    def __repr__(self):
        return flow.name(self._actor, **self._params)

    @abc.abstractmethod
    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns:
            True if the wrapped actor is stateful (has a train method).
        """

    def spec(self, *args, **kwargs) -> flow.Spec:
        """Shortcut for creating a spec of this actor.

        Args:
            *args: Args to be used for the spec.
            **kwargs: Keywords to be used for the spec.

        Returns:
            Actor spec instance.
        """
        return flow.Spec(self, *args, **kwargs)


class Mapping(Proxy[ActorT, Target]):
    """Base class for actor wrapping."""

    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns:
            True if the wrapped actor is stateful (has a train method).
        """
        attr = self._params[flow.Actor.train.__name__]
        return callable(attr) or hasattr(self._actor, attr)


class Class(Mapping[type]):
    """Decorator wrapper."""

    class Actor(Mapping[object], flow.Actor):  # pylint: disable=abstract-method
        """Wrapper around user class implementing the Actor interface."""

        class Decorated:
            """Decorated representation of the mapping target."""

            def __init__(self, instance: object, decorator: typing.Callable[..., typing.Any]):
                self._instance: object = instance
                self._decorator: typing.Callable[..., typing.Any] = decorator

            def __call__(self, *args, **kwargs):
                return self._decorator(self._instance, *args, **kwargs)

        def __new__(cls, actor: object, params: typing.Mapping[str, Target]):  # pylint: disable=unused-argument
            cls.__abstractmethods__ = frozenset()
            return super().__new__(cls)

        def __init__(self, actor: object, params: typing.Mapping[str, Target]):
            super().__init__(actor, params)

        def __getnewargs__(self):
            return self._actor, self._params

        def __getattribute__(self, item):
            if not item.startswith('_'):
                if item in self._params:
                    attr = self._params[item]
                    return self.Decorated(self._actor, attr) if callable(attr) else getattr(self._actor, attr)
                if hasattr(self._actor, item):
                    return getattr(self._actor, item)
            return super().__getattribute__(item)

    def __init__(self, actor: type, params: typing.Mapping[str, Target]):
        assert not issubclass(actor, flow.Actor), 'Already an actor'
        super().__init__(actor, params)

    def __call__(self, *args, **kwargs) -> flow.Actor:
        return self.Actor(self._actor(*args, **kwargs), self._params)  # pylint: disable=abstract-class-instantiated

    def __repr__(self):
        return flow.name(self._actor)

    @staticmethod
    def actor(  # pylint: disable=bad-staticmethod-argument
        cls: typing.Optional[type] = None, /, **mapping: Target
    ) -> type[flow.Actor]:
        """Decorator for turning an user class to a valid actor. This can be used either as parameterless decorator or
        optionally with mapping of Actor methods to decorated user class implementation.

        Args:
            cls: Decorated class.
            apply: Method name or decorator function implementing the actor apply.
            train: Method name or decorator function implementing the actor train.
            get_params: Method name or decorator function implementing the actor get_params.
            set_params: Method name or decorator function implementing the actor set_params.

        Returns:
            Actor class.
        """
        if not all(
            isinstance(a, (str, typing.Callable))  # pylint: disable=isinstance-second-argument-not-valid-type
            for a in mapping.values()
        ):
            raise ValueError('Invalid mapping')

        for method in (flow.Actor.apply, flow.Actor.train, flow.Actor.get_params, flow.Actor.set_params):
            mapping.setdefault(method.__name__, method.__name__)

        def decorator(cls) -> type[flow.Actor]:
            """Decorating function."""
            if not inspect.isclass(cls):
                raise ValueError(f'Invalid actor class {cls}')
            if issubclass(cls, flow.Actor):
                return cls
            for target in {t for s, t in mapping.items() if s != flow.Actor.train.__name__ and not callable(t)}:
                if not callable(getattr(cls, target, None)):
                    raise forml.MissingError(f'Wrapped actor missing required {target} implementation')
            return Class(cls, mapping)

        if cls:
            decorator = decorator(cls)
        return decorator


class Function(Proxy[typing.Callable[[typing.Any], typing.Any], typing.Any]):
    """Function wrapping actor."""

    class Actor(flow.Actor):
        """Wrapper around user class implementing the Actor interface."""

        def __init__(
            self, function: typing.Callable[[typing.Any], typing.Any], *args: typing.Any, **kwargs: typing.Any
        ):
            # poor-man's args validation against the function signature; this works only partially as we don't know
            # how many of the function arguments are data input ports (at least one but possibly more)
            signature = inspect.signature(function)
            signature.replace(parameters=itertools.islice(signature.parameters.values(), 1, None)).bind_partial(
                *args, **kwargs
            )
            self._function: typing.Callable[[typing.Any], typing.Any] = function
            self._args: typing.Sequence[typing.Any] = args
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __repr__(self):
            return flow.name(self._function, *self._args, **self._kwargs)

        def apply(self, *features: typing.Any) -> typing.Union[typing.Any, typing.Sequence[typing.Any]]:
            return self._function(*features, *self._args, **self._kwargs)

        def get_params(self) -> dict[str, typing.Any]:
            """Standard param getter.

            Returns:
                Evaluation function.
            """
            return {'function': self._function, 'args': self._args, 'kwargs': dict(self._kwargs)}

        def set_params(
            self,  # pylint: disable=arguments-differ
            function: typing.Optional[typing.Callable[[typing.Any], float]] = None,
            args: typing.Optional[typing.Sequence[typing.Any]] = None,
            kwargs: typing.Optional[dict[str, typing.Any]] = None,
        ) -> None:
            """Standard params setter.

            Args:
                function: Evaluation function.
                args: Extra positional args.
                kwargs: Extra kwargs to be passed to function.
            """
            if function:
                self._function = function
            if args:
                self._args = args
            if kwargs:
                self._kwargs = kwargs

    def __init__(self, function: typing.Callable[[typing.Any], typing.Any], **params: typing.Any):
        if not inspect.isfunction(function):
            raise ValueError(f'Invalid actor function {function}')
        super().__init__(function, params)

    def __call__(self, *args, **kwargs) -> 'Function.Actor':
        return self.Actor(self._actor, *args, **self._params | kwargs)

    def is_stateful(self) -> bool:
        """Wrapped function is generally stateless.

        Returns:
            False
        """
        return False

    @staticmethod
    def actor(
        function: typing.Optional[typing.Callable[[typing.Any], typing.Any]] = None, /, **params: typing.Any
    ) -> type[flow.Actor]:
        """Decorator for turning an user function to a valid actor. This can be used either as parameterless decorator
        or optionally with addition kwargs that will be passed to the function.

        Args:
            function: Decorated function.
            params: Optional kwargs to be passed to function.

        Returns:
            Actor class.
        """

        def decorator(function) -> type[flow.Actor]:
            """Decorating function."""
            return Function(function, **params)

        if function:
            decorator = decorator(function)
        return decorator
