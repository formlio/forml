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

from . import _proxy

LOGGER = logging.getLogger(__name__)


class Alike(typing.Generic[_proxy.Origin], metaclass=abc.ABCMeta):
    """Base class for Actor-like wrappers."""

    def __init__(self, origin: _proxy.Origin):
        self._origin: _proxy.Origin = origin

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


class Mapping(Alike[_proxy.Origin]):
    """Base class for mapped actor wrapping."""

    Target = typing.Union[str, typing.Callable[..., typing.Any]]

    def __init__(self, origin: _proxy.Origin, mapping: typing.Mapping[str, Target]):
        super().__init__(origin)
        self._mapping: typing.Mapping[str, Mapping.Target] = dict(mapping)

    def __reduce__(self):
        return self.__class__, (self._origin, self._mapping)

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


class Type(
    typing.Generic[_proxy.Origin], Alike[_proxy.Origin], _proxy.Type[_proxy.Origin, flow.Builder], metaclass=abc.ABCMeta
):
    """Fake Actor type that produces Actor instance in constructor-like fashion."""

    def __init__(self, origin: _proxy.Origin):
        Alike.__init__(self, origin)
        _proxy.Type.__init__(self, origin)

    def builder(self, *args, **kwargs) -> flow.Builder:
        """Shortcut for creating a builder of this actor builder.

        Args:
            *args: Args to be used for the builder.
            **kwargs: Keywords to be used for the builder.

        Returns:
            Actor builder instance.
        """
        return flow.Builder(self, *args, **kwargs)


class Class(Mapping[type], Type[type]):
    """Wrapped class based actor :class:`type-like <forml.pipeline.wrap.Type>` proxy."""

    class Actor(Mapping[object], flow.Actor):  # pylint: disable=abstract-method
        """Wrapper around user class implementing the Actor interface."""

        class Decorated:
            """Decorated representation of the mapping target."""

            def __init__(self, instance: object, decorator: typing.Callable[..., typing.Any]):
                self._instance: object = instance
                self._decorator: typing.Callable[..., typing.Any] = decorator

            def __call__(self, *args, **kwargs):
                return self._decorator(self._instance, *args, **kwargs)

        def apply(self, *features: 'flow.Features') -> 'flow.Result':
            return self._mapping['apply'](*features)

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
    """Stateless function based actor :class:`type-like <forml.pipeline.wrap.Type>` proxy."""

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
    """Stateful function based actor :class:`type-like <forml.pipeline.wrap.Type>` proxy."""

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
                return b''
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
    """Central class providing decorators/wrappers for creating ForML *Actors* using a number of
    convenient ways not requiring to fully implement the :class:`flow.Actor <forml.flow.Actor>` base
    class from scratch.

    .. rubric:: Decorator Methods

    Methods:
        apply(origin):
            Decorator for turning a given plain function into a *stateless* Actor.

            Args:
                origin: Decorated function.

                    The function must have one of the following signatures::

                        def foo(*features: flow.Features) -> flow.Result:
                        def foo(features: flow.Features) -> flow.Result:
                        def foo(*features: flow.Features, opt1, optN=None) -> flow.Result:
                        def foo(features: flow.Features, *, opt1, optN=None) -> flow.Result:
                        def foo(*features: flow.Features, opt1, **kwargs) -> flow.Result:
                        def foo(features: flow.Features, /, *, opt1, **kwargs) -> flow.Result:

                    Attention:
                        The optional arguments ``opt1``, ``opt2`` and ``**kwargs`` must all be
                        *keyword-only* arguments.

            Returns:
                :class:`Actor-type-like object <forml.pipeline.wrap.Type>` that can be instantiated
                into a *stateless* Actor with the given *apply* logic.

            Examples:
                Simple stateless imputation actor using the provided value to fill the NaNs::

                    @wrap.Actor.apply
                    def StaticImpute(
                        df: pandas.DataFrame,
                        *,
                        column: str,
                        value: float,
                    ) -> pandas.DataFrame:
                        df[column] = df[column].fillna(value)
                        return df

        train(origin):
            Decorator for turning a given plain function into a follow-up apply function decorator.

            Stateful actors need to have distinct implementations for their :ref:`train vs apply
            modes <workflow-mode>`. This wrapping faciality achieves that by decorating two
            companion functions each implementing the relevant mode.

            Args:
                origin: Decorated train function.

                    The decorated *train* function must have one of the following signatures::

                        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels) -> State:
                        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels, opt1, optN=None) -> State:
                        def foo(state: typing.Optional[State], features: flow.Features, labels: flow.Labels, /, opt1,**kwargs) -> State:

                    The function will receive the *previous state* as the first parameter and is
                    expected to provide the *new state* instance as its return value.

            Returns:
                Follow-up *decorator* to be used for wrapping the companion *apply* function which
                eventually returns an :class:`actor-type-like object <forml.pipeline.wrap.Type>`
                that can be instantiated into a *stateful* Actor with the given *train-apply* logic.

                The decorated *apply* function must have one of the following signatures::

                    def foo(state: State, features: flow.Features) -> flow.Result:
                    def foo(state: State, features: flow.Features, opt1, optN=None) -> flow.Result:
                    def foo(state: State, features: flow.Features, /, opt1, **kwargs) -> flow.Result:

                The function will receive the *current state* as the first parameter and is
                expected to provide the *apply-mode* transformation result.

            Examples:
                Simple stateful imputation actor using the trained mean value to fill the NaNs::

                    @wrap.Actor.train  # starting with wrapping the train-mode function
                    def MeanImpute(
                        state: typing.Optional[float],  # receving the previous state (not used)
                        features: pandas.DataFrame,
                        labels: pandas.Series,
                        *,
                        column: str,
                    ) -> float:
                        return features[column].mean()  # returning the new state

                    @MeanImpute.apply  # continue with the follow-up apply-mode function decorator
                    def MeanImpute(
                        state: float,  # receiving current state
                        features: pandas.DataFrame,
                        *,
                        column: str
                    ) -> pandas.DataFrame:
                        features[column] = features[column].fillna(state)
                        return features  # apply-mode result

        type(origin=None, /, *, apply=None, train=None, get_params=None, set_params=None):
            Wrapper for turning an external user class to a valid Actor.

            This can be used either as parameterless decorator or optionally with mapping of Actor
            methods to decorated user class implementation.

            Args:
                origin: Decorated class.
                apply: Target method name or decorator function implementing the actor
                       :meth:`apply <forml.flow.Actor.apply>` logic.
                train: Target method name or decorator function implementing the actor
                       :meth:`train <forml.flow.Actor.train>` logic.
                get_params: Target method name or decorator function implementing the actor
                            :meth:`get_params <forml.flow.Actor.get_params>` logic.
                set_params: Target method name or decorator function implementing the actor
                            :meth:`set_params <forml.flow.Actor.set_params>` logic.

            Returns:
                Actor class.

            Examples:
                >>> RfcActor = wrap.Actor.type(
                ...     sklearn.ensemble.RandomForestClassifier,
                ...     train='fit',
                ...     apply=lambda c, *a, **kw: c.predict_proba(*a, **kw).transpose()[-1],
                ... )
    """  # pylint: disable=line-too-long  # noqa: E501

    class Train:
        """Follow-up train function decorator."""

        def __init__(self, origin: typing.Callable[..., State]):
            self._origin: typing.Callable[..., State] = origin

        def apply(self, origin: typing.Callable[..., flow.Result], /) -> type[flow.Actor]:
            """Apply function decorator (following-up from a train function decorator) turning it
            into a stateful Actor.

            See Also: Full description in the class docstring.
            """
            return Stateful(self._origin, origin)

    @classmethod
    def apply(cls, origin: typing.Callable[..., flow.Result], /) -> type[flow.Actor]:
        """Function decorator turning it into a stateless Actor.

        See Also: Full description in the class docstring.
        """
        return Stateless(origin)

    @classmethod
    def train(cls, origin: typing.Callable[..., State], /) -> 'Actor.Train':
        """Train function decorator turning it into a follow-up apply function decorator.

        See Also: Full description in the class docstring.
        """
        return cls.Train(origin)

    @classmethod
    def type(cls, origin: typing.Optional[type] = None, /, **mapping: Mapping.Target) -> type[flow.Actor]:
        """Decorator for turning an external user class to a valid actor.

        See Also: Full description in the class docstring.
        """

        def decorator(origin: type) -> type[flow.Actor]:
            """Decorating function."""
            return Class(origin, mapping)

        if origin:
            decorator = decorator(origin)
        return decorator

    def __new__(cls, *args, **kwargs):
        raise TypeError(f'Illegal attempt instantiating {cls.__name__}.')
