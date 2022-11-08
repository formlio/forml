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
import copyreg
import functools
import inspect
import itertools
import logging
import types
import typing

import cloudpickle

from forml import flow

LOGGER = logging.getLogger(__name__)


class Class(abc.ABCMeta):
    """Wrapped class-based actor metaclass."""

    Target = typing.Union[str, typing.Callable[..., typing.Any]]

    class Actor(flow.Actor[flow.Features, flow.Labels, flow.Result], metaclass=abc.ABCMeta):
        """Wrapper around user class implementing the Actor interface."""

        class Decorated:
            """Decorated representation of the mapping target."""

            def __init__(self, instance: object, decorator: typing.Callable[..., typing.Any]):
                self._instance: object = instance
                self._decorator: typing.Callable[..., typing.Any] = decorator

            def __call__(self, *args, **kwargs):
                return self._decorator(self._instance, *args, **kwargs)

        class Origin(abc.ABC):
            """Wrapped origin class (to be injected by metaclass)."""

        @property
        @abc.abstractmethod
        def Mapping(self) -> typing.Mapping[str, 'Class.Target']:  # pylint: disable=invalid-name
            """Mapping from the Actor API to the Origin API (to be injected by metaclass)."""

        def __init__(self, *args, **kwargs):
            self._origin = self.Origin(*args, **kwargs)

        def apply(self, *features: flow.Features) -> flow.Result:
            return self.Mapping['apply'](*features)

        @classmethod
        def is_stateful(cls) -> bool:
            attr = cls.Mapping[flow.Actor.train.__name__]
            return callable(attr) or hasattr(cls.Origin, attr)

        def __getattribute__(self, item):
            if item not in {'Origin', 'Mapping', '_origin'}:
                if item in self.Mapping:
                    attr = self.Mapping[item]
                    return self.Decorated(self._origin, attr) if callable(attr) else getattr(self._origin, attr)
                if hasattr(self._origin, item):
                    return getattr(self._origin, item)
            return super().__getattribute__(item)

    def __new__(
        mcs,
        name: str,
        bases: tuple[type],
        namespace: dict[str, typing.Any],
        *,
        origin: typing.Optional[type] = None,
        mapping: typing.Optional[typing.Mapping[str, 'Class.Target']] = None,
    ):
        if origin:
            assert not issubclass(origin, flow.Actor), 'Already an actor'
        if mapping:
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
            mapping = types.MappingProxyType(mapping)

        actor = super().__new__(
            mcs,
            name,
            (mcs.Actor,),
            {mcs.Actor.Origin.__name__: origin, mcs.Actor.Mapping.fget.__name__: mapping},
        )
        actor = functools.update_wrapper(actor, origin, updated=())
        copyreg.pickle(
            actor,
            lambda a: (
                actor,
                (),
                (a.get_state(), a.get_params()),
                None,
                None,
                lambda o, s: (o.set_state(s[0]), o.set_params(**s[1])),
            ),
        )
        return actor


class Parametric(flow.Actor[flow.Features, flow.Labels, flow.Result], metaclass=abc.ABCMeta):
    """Base class for function based actors."""

    def __init__(self, **kwargs: typing.Any):
        self._kwargs: dict[str, typing.Any] = kwargs

    def get_params(self) -> typing.Mapping[str, typing.Any]:
        return dict(self._kwargs)

    def set_params(self, **kwargs) -> None:
        self._kwargs.update(kwargs)


class Stateless(abc.ABCMeta):
    """Stateless function-based actor metaclass."""

    class Actor(Parametric[flow.Features, None, flow.Result], metaclass=abc.ABCMeta):
        """Stateless actor based on the given function."""

        @staticmethod
        @abc.abstractmethod
        def Apply(*features, **kwargs) -> flow.Result:  # pylint: disable=invalid-name
            """Wrapped origin apply function (to be injected by metaclass)."""

        def __init__(self, **kwargs: typing.Any):
            signature = inspect.signature(self.Apply)
            params = {
                p
                for p in signature.parameters.values()
                if p.kind in {inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD}
            }
            # validating the kwonly params - the rest is expected to be *features
            signature.replace(parameters=params).bind(**kwargs)
            super().__init__(**kwargs)

        def apply(self, *features: flow.Features) -> flow.Result:
            return self.Apply(*features, **self._kwargs)

    def __new__(
        mcs,
        name: str,
        bases: tuple[type],
        namespace: dict[str, typing.Any],
        *,
        apply: typing.Optional[typing.Callable[..., flow.Result]] = None,
    ):

        actor = super().__new__(
            mcs,
            name,
            (mcs.Actor,),
            {mcs.Actor.Apply.__name__: staticmethod(apply)},
        )
        return functools.update_wrapper(actor, apply, updated=())


State = typing.TypeVar('State')


class Stateful(abc.ABCMeta):
    """Stateful function-based actor metaclass."""

    class Actor(
        typing.Generic[State, flow.Features, flow.Labels, flow.Result],
        Parametric[flow.Features, flow.Labels, flow.Result],
        metaclass=abc.ABCMeta,
    ):
        """Stateful actor based on the given functions."""

        @staticmethod
        @abc.abstractmethod
        def Apply(state: State, features: flow.Features, **kwargs) -> flow.Result:  # pylint: disable=invalid-name
            """Wrapped origin apply function (to be injected by metaclass)."""

        @staticmethod
        @abc.abstractmethod
        def Train(  # pylint: disable=invalid-name
            state: typing.Optional[State], features: flow.Features, labels: flow.Labels, **kwargs
        ) -> State:
            """Wrapped origin train function (to be injected by metaclass)."""

        def __init__(self, **kwargs):
            # validating the kwargs against the signatures
            # skipping initial args - state+features+labels for train mode and state+features for apply mode
            for skip, origin in (3, self.Train), (2, self.Apply):
                signature = inspect.signature(origin)
                params = itertools.islice(signature.parameters.values(), skip, None)
                signature.replace(parameters=params).bind(**kwargs)
            self._state: typing.Optional[State] = None
            super().__init__(**kwargs)

        def train(self, features: flow.Features, labels: flow.Labels, /) -> None:
            state = self.Train(self._state, features, labels, **self._kwargs)
            if state is None:
                LOGGER.warning('Stateful function-based actor returned None state - not considered trained')
            self._state = state

        def apply(self, features: flow.Features) -> flow.Result:
            if self._state is None:
                raise RuntimeError('Actor not trained')
            return self.Apply(self._state, features, **self._kwargs)

        def get_state(self) -> bytes:
            if self._state is None:
                return b''
            return cloudpickle.dumps(self._state)

        def set_state(self, state: bytes) -> None:
            if state:
                self._state = cloudpickle.loads(state)

    def __new__(
        mcs,
        name: str,
        bases: tuple[type],
        namespace: dict[str, typing.Any],
        *,
        train: typing.Optional[typing.Callable[..., State]] = None,
        apply: typing.Optional[typing.Callable[..., flow.Result]] = None,
    ):

        actor = super().__new__(
            mcs,
            name,
            (mcs.Actor,),
            {mcs.Actor.Apply.__name__: staticmethod(apply), mcs.Actor.Train.__name__: staticmethod(train)},
        )
        return functools.update_wrapper(actor, apply, updated=())


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
                        The optional arguments ``opt1``, ``opt2``, and ``**kwargs`` must all be
                        *keyword-only* arguments.

            Returns:
                A *stateless* Actor class with the given *apply* logic.

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
            modes <workflow-mode>`. This wrapping facility achieves that by decorating two
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
                eventually returns a *stateful* Actor class with the given *train-apply* logic.

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
            Wrapper for turning an external user class into a valid Actor.

            This can be used either as a parameterless decorator or optionally with mapping of Actor
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
            if not inspect.isfunction(origin):
                raise TypeError(f'Invalid actor function {origin}')
            return Stateful(origin.__name__, (), {}, train=self._origin, apply=origin)

    @classmethod
    def apply(cls, origin: typing.Callable[..., flow.Result], /) -> type[flow.Actor]:
        """Function decorator turning it into a stateless Actor.

        See Also: Full description in the class docstring.
        """
        if not inspect.isfunction(origin):
            raise TypeError(f'Invalid actor function {origin}')
        return Stateless(origin.__name__, (), {}, apply=origin)

    @classmethod
    def train(cls, origin: typing.Callable[..., State], /) -> 'Actor.Train':
        """Train function decorator turning it into a follow-up apply function decorator.

        See Also: Full description in the class docstring.
        """
        if not inspect.isfunction(origin):
            raise TypeError(f'Invalid actor function {origin}')
        return cls.Train(origin)

    @classmethod
    def type(cls, origin: typing.Optional[type] = None, /, **mapping: Class.Target) -> type[flow.Actor]:
        """Decorator for turning an external user class into a valid actor.

        See Also: Full description in the class docstring.
        """

        def decorator(origin: type) -> type[flow.Actor]:
            """Decorating function."""
            if not inspect.isclass(origin):
                raise TypeError(f'Invalid actor class {origin}')
            return Class(origin.__name__, (), {}, origin=origin, mapping=mapping)

        if origin:
            decorator = decorator(origin)
        return decorator

    def __new__(cls, *args, **kwargs):
        raise TypeError(f'Illegal attempt instantiating {cls.__name__}.')
