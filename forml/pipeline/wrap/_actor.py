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
import logging
import typing

import cloudpickle

from forml import flow

LOGGER = logging.getLogger(__name__)
Origin = typing.TypeVar('Origin')


class Proxy(typing.Generic[Origin], metaclass=abc.ABCMeta):
    """Base class for Actor wrappers."""

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


class Mapping(Proxy[Origin]):
    """Base class for actor wrapping."""

    Target = typing.Union[str, typing.Callable[..., typing.Any]]

    def __init__(self, origin: Origin, mapping: typing.Mapping[str, Target]):
        super().__init__(origin)
        self._mapping: typing.Mapping[str, Mapping.Target] = dict(mapping)

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


class Type(Proxy[Origin], metaclass=abc.ABCMeta):
    """Fake Actor type that produces Actor instance in constructor-like fashion."""

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> flow.Actor:
        """Actor constructor-like proxy."""

    def spec(self, *args, **kwargs) -> flow.Spec:
        """Shortcut for creating a spec of this actor builder.

        Args:
            *args: Args to be used for the spec.
            **kwargs: Keywords to be used for the spec.

        Returns:
            Actor spec instance.
        """
        return flow.Spec(self, *args, **kwargs)


class Class(Mapping[type], Type[type]):
    """Wrapped class based actor type-like proxy."""

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
        if not inspect.isclass(origin):
            raise TypeError(f'Invalid actor class {origin}')
        assert not issubclass(origin, flow.Actor), 'Already an actor'
        mapping = dict(mapping)
        if not all(
            isinstance(a, (str, typing.Callable))  # pylint: disable=isinstance-second-argument-not-valid-type
            for a in mapping.values()
        ):
            raise TypeError('Invalid mapping')
        for method in (flow.Actor.apply, flow.Actor.train, flow.Actor.get_params, flow.Actor.set_params):
            mapping.setdefault(method.__name__, method.__name__)
        for target in {t for s, t in mapping.items() if s != flow.Actor.train.__name__ and not callable(t)}:
            if not callable(getattr(origin, target, None)):
                raise TypeError(f'Wrapped actor missing required {target} implementation')

        super().__init__(origin, mapping)

    def __call__(self, *args, **kwargs) -> flow.Actor:
        return self.Actor(self._origin(*args, **kwargs), self._mapping)  # pylint: disable=abstract-class-instantiated


class Parametric(flow.Actor[flow.Features, flow.Labels, flow.Result], metaclass=abc.ABCMeta):
    """Base class for function based actors."""

    def __init__(self, **kwargs: typing.Any):
        self._kwargs: dict[str, typing.Any] = kwargs

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        return dict(self._kwargs)

    def set_params(self, **kwargs) -> None:
        self._kwargs.update(kwargs)


class Stateless(Type[typing.Callable[..., flow.Result]]):
    """Stateless function based actor type-like proxy."""

    class Actor(Parametric[flow.Features, None, flow.Result]):
        """Stateless actor based on the given function."""

        def __init__(self, origin: typing.Callable[..., flow.Result], /, **kwargs: typing.Any):
            signature = inspect.signature(origin)
            params = {
                p
                for p in signature.parameters.values()
                if p.kind in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD}
            }
            # validating the kwonly params - the rest is expected to be *features
            signature.replace(parameters=params).bind(**kwargs)
            self._origin: typing.Callable[..., flow.Result] = origin
            super().__init__(**kwargs)

        def apply(self, *features: flow.Features) -> flow.Result:
            return self._origin(*features, **self._kwargs)

    def __init__(self, origin: typing.Callable[..., flow.Result]):
        if not inspect.isfunction(origin):
            raise TypeError(f'Invalid actor function {origin}')
        super().__init__(origin)

    def __call__(self, **kwargs) -> flow.Actor:
        return self.Actor(self._origin, **kwargs)

    def is_stateful(self) -> bool:
        return False


State = typing.TypeVar('State')


class Stateful(Type[tuple[typing.Callable[..., State], typing.Callable[..., flow.Result]]]):
    """Stateful function based actor type-like proxy."""

    class Actor(
        typing.Generic[State, flow.Features, flow.Labels, flow.Result],
        Parametric[flow.Features, flow.Labels, flow.Result],
    ):
        """Stateful actor based on the given functions."""

        def __init__(self, train: typing.Callable[..., State], apply: typing.Callable[..., flow.Result], /, **kwargs):
            # validating the kwargs against the signatures
            # skipping initial args - state+features+labels for train mode and state+features for apply mode
            for skip, origin in (3, train), (2, apply):
                signature = inspect.signature(origin)
                params = itertools.islice(signature.parameters.values(), skip, None)
                signature.replace(parameters=params).bind(**kwargs)
            self._train: typing.Callable[..., State] = train
            self._apply: typing.Callable[..., flow.Result] = apply
            self._state: typing.Optional[State] = None
            super().__init__(**kwargs)

        def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
            state = self._train(self._state, features, labels, **self._kwargs)
            if state is None:
                LOGGER.warning('Stateful function based actor returned None state - not considered trained')
            self._state = state

        def apply(self, features: flow.Features) -> flow.Result:
            if self._state is None:
                raise RuntimeError('Actor not trained')
            return self._apply(self._state, features, **self._kwargs)

        def get_state(self) -> bytes:
            if self._state is None:
                return bytes()
            return cloudpickle.dumps(self._state)

        def set_state(self, state: bytes) -> None:
            if state:
                self._state = cloudpickle.loads(state)

    def __init__(self, train: typing.Callable[..., State], apply: typing.Callable[..., flow.Result]):
        for origin in train, apply:
            if not inspect.isfunction(origin):
                raise TypeError(f'Invalid actor function {origin}')
        super().__init__((train, apply))

    def __repr__(self):
        return flow.name(self._origin[1])

    def __call__(self, **kwargs) -> flow.Actor:
        train, apply = self._origin
        return self.Actor(train, apply, **kwargs)

    def is_stateful(self) -> bool:
        return True


class Actor:
    """Actor wrappers."""

    class Train:
        """Follow-up train function decorator."""

        def __init__(self, origin: typing.Callable[..., State]):
            self._origin: typing.Callable[..., State] = origin

        def apply(self, origin: typing.Callable[..., flow.Result], /) -> type[flow.Actor]:
            """Apply function decorator (following-up from a train function decorator) turning it into a stateful Actor.

            The decorated function should have one of the following signatures:

            def foo(state: State, features: flow.Features) -> flow.Result:
            def foo(state: State, features: flow.Features, opt1, optN=None) -> flow.Result:
            def foo(state: State, features: flow.Features, /, opt1, **kwargs) -> flow.Result:

            Args:
                origin: Decorated apply function.

            Returns:
                Actor type-like object that can be instantiated into a stateful Actor with the given train-apply logic.
            """
            return Stateful(self._origin, origin)

    @classmethod
    def apply(cls, origin: typing.Callable[..., flow.Result], /) -> type[flow.Actor]:
        """Function decorator turning it into a stateless Actor.

        The decorated function should have one of the following signatures:

        def foo(*features: flow.Features) -> flow.Result:
        def foo(features: flow.Features) -> flow.Result:
        def foo(*features: flow.Features, opt1, optN=None) -> flow.Result:
        def foo(features: flow.Features, *, opt1, optN=None) -> flow.Result:
        def foo(*features: flow.Features, opt1, **kwargs) -> flow.Result:
        def foo(features: flow.Features, /, *, opt1, **kwargs) -> flow.Result:

        The optional arguments opt1, opt2, **kwargs must all be keyword-only arguments.

        Args:
            origin: Decorated apply function.

        Returns:
            Actor type-like object that can be instantiated into a stateless Actor with the given apply logic.
        """
        return Stateless(origin)

    @classmethod
    def train(cls, origin: typing.Callable[..., State], /) -> 'Actor.Train':
        """Train function decorator turning it into a follow-up apply function decorator.

        The decorated function should have one of the following signatures:

        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels) -> State:
        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels, opt1, optN=None) -> State:
        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels, /, opt1,**kwargs) -> State:

        Args:
            origin: Decorated train function.

        Returns:
            Follow-up apply function decorator.
        """
        return cls.Train(origin)

    @classmethod
    def type(cls, origin: typing.Optional[type] = None, /, **mapping: Mapping.Target) -> type[flow.Actor]:
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

        def decorator(origin: type) -> type[flow.Actor]:
            """Decorating function."""
            return Class(origin, mapping)

        if origin:
            decorator = decorator(origin)
        return decorator
