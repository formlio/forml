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
import typing

import forml
from forml import flow

Origin = typing.TypeVar('Origin')


class Proxy(typing.Generic[Origin], metaclass=abc.ABCMeta):
    """Base class for wrappers."""

    def __init__(self, origin: Origin):
        self._origin: Origin = origin

    def __hash__(self):
        return hash(self._origin)

    def __eq__(self, other: typing.Any):
        return isinstance(other, self.__class__) and self._origin == other._origin  # pylint: disable=protected-access

    def __repr__(self):
        return flow.name(self._origin)

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


class Mapping(Proxy[Origin]):
    """Base class for actor wrapping."""

    Target = typing.Union[str, typing.Callable[..., typing.Any]]

    def __init__(self, origin: Origin, mapping: typing.Mapping[str, Target]):
        super().__init__(origin)
        self._mapping: typing.Mapping[str, Mapping.Target] = mapping

    def __hash__(self):
        return super().__hash__() ^ hash(tuple(sorted(self._mapping.items())))

    def __eq__(self, other: typing.Any):
        return super().__eq__(other) and self._mapping == other._mapping  # pylint: disable=protected-access

    def is_stateful(self) -> bool:
        """Emulation of native actor is_stateful class method.

        Returns:
            True if the wrapped actor is stateful (has a train method).
        """
        attr = self._mapping[flow.Actor.train.__name__]
        return callable(attr) or hasattr(self._origin, attr)


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

        def __new__(
            cls, actor: object, mapping: typing.Mapping[str, Mapping.Target]
        ):  # pylint: disable=unused-argument
            cls.__abstractmethods__ = frozenset()
            return super().__new__(cls)

        def __init__(self, actor: object, mapping: typing.Mapping[str, Mapping.Target]):
            super().__init__(actor, mapping)

        def __getnewargs__(self):
            return self._origin, self._mapping

        def __getattribute__(self, item):
            if not item.startswith('_'):
                if item in self._mapping:
                    attr = self._mapping[item]
                    return self.Decorated(self._origin, attr) if callable(attr) else getattr(self._origin, attr)
                if hasattr(self._origin, item):
                    return getattr(self._origin, item)
            return super().__getattribute__(item)

    def __init__(self, origin: type, mapping: typing.Mapping[str, Mapping.Target]):
        assert not issubclass(origin, flow.Actor), 'Already an actor'
        super().__init__(origin, mapping)

    def __call__(self, *args, **kwargs) -> flow.Actor:
        return self.Actor(self._origin(*args, **kwargs), self._mapping)  # pylint: disable=abstract-class-instantiated

    @classmethod
    def actor(cls, origin: typing.Optional[type] = None, /, **mapping: Mapping.Target) -> type[flow.Actor]:
        """Decorator for turning a user class to a valid actor. This can be used either as parameterless decorator or
        optionally with mapping of Actor methods to decorated user class implementation.

        Args:
            origin: Decorated class.
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

        def decorator(origin: type) -> type[flow.Actor]:
            """Decorating function."""
            if not inspect.isclass(origin):
                raise ValueError(f'Invalid actor class {origin}')
            if issubclass(origin, flow.Actor):
                return origin
            for target in {t for s, t in mapping.items() if s != flow.Actor.train.__name__ and not callable(t)}:
                if not callable(getattr(origin, target, None)):
                    raise forml.MissingError(f'Wrapped actor missing required {target} implementation')
            return cls(origin, mapping)

        if origin:
            decorator = decorator(origin)
        return decorator


class Function(Proxy[typing.Callable[..., typing.Any]]):
    """Function wrapping actor."""

    class Actor(flow.Actor[flow.Features, flow.Labels, flow.Result]):
        """Wrapper around user class implementing the Actor interface."""

        def __init__(self, function: typing.Callable[..., flow.Result], /, **kwargs: typing.Any):
            signature = inspect.signature(function)
            params = {
                p
                for p in signature.parameters.values()
                if p.kind in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD}
            }
            signature.replace(parameters=params).bind(**kwargs)
            self._function: typing.Callable[..., flow.Result] = function
            self._kwargs: typing.Mapping[str, typing.Any] = kwargs

        def __repr__(self):
            return flow.name(self._function, **self._kwargs)

        def apply(self, *features: flow.Features) -> flow.Result:
            return self._function(*features, **self._kwargs)

        def get_params(self) -> dict[str, typing.Any]:
            """Standard param getter.

            Returns:
                Evaluation function.
            """
            return {'function': self._function, 'kwargs': dict(self._kwargs)}

        def set_params(
            self,  # pylint: disable=arguments-differ
            function: typing.Optional[typing.Callable[[typing.Any], float]] = None,
            kwargs: typing.Optional[dict[str, typing.Any]] = None,
        ) -> None:
            """Standard params setter.

            Args:
                function: Evaluation function.
                kwargs: Extra kwargs to be passed to function.
            """
            if function:
                self._function = function
            if kwargs:
                self._kwargs = kwargs

    def __init__(self, origin: typing.Callable[..., typing.Any], /):
        if not inspect.isfunction(origin):
            raise ValueError(f'Invalid actor function {origin}')
        super().__init__(origin)

    def __call__(self, **kwargs) -> 'Function.Actor':  # Actor constructor-like
        return self.Actor(self._origin, **kwargs)

    def is_stateful(self) -> bool:
        """Wrapped function is generally stateless.

        Returns:
            False
        """
        return False

    @classmethod
    def actor(cls, origin: typing.Callable[..., typing.Any], /) -> type[flow.Actor]:
        """Decorator for turning a user function to a valid actor.

        Args:
            origin: Decorated function.

        Returns:
            Actor class.
        """

        return cls(origin)
