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
Generic operators.
"""

import abc
import typing

import forml
from forml import flow


class Adapter(flow.Operator, metaclass=abc.ABCMeta):
    """Adapter is a generic stateless transformer applied to specific pipeline mode(s).

    Actor definitions for individual modes can be defined using convenient decorators:

    @Adapter.train(**kwargs)  # optional kwargs will be passed to actor
    @Adapter.apply  # decorators can be chained if same actor is supposed to be used for another mode
    @topology.Function.actor
    def myadapter(df, **kwargs):
        # stateless adapter implementation used for train/apply paths

    @myadapter.label(**kwargs)  # previously decorated adapter can be used as decorator itself to override certain mode
    @topology.Function.actor
    def myadapter(df, **kwargs):
        # stateless adapter implementation used for label path
    """

    class Builder:
        """Adapter builder carrying the parameters during decorations."""

        class Decorator:
            """Builder-level decorator used for overriding specific modes on previously decorated instances."""

            def __init__(self, builder: 'Adapter.Builder', setter: 'Adapter.Builder.Setter'):
                self._builder: Adapter.Builder = builder
                self._setter: Adapter.Builder.Setter = setter

            def __call__(
                self, actor: typing.Optional[type[flow.Actor]] = None, /, **params: typing.Any
            ) -> typing.Union['Adapter.Builder', typing.Callable[[type[flow.Actor]], 'Adapter.Builder']]:
                def decorator(actor: type[flow.Actor]) -> 'Adapter.Builder':
                    """Decorating function."""
                    self._setter(actor, **params)
                    return self._builder

                if actor:  # we are already a decorator
                    return decorator(actor)
                # we are either parametrized decorator or just a builder setter
                self._setter(**params)  # in case we are just a setter
                return decorator

        class Setter:
            """Helper for setting/holding the config parameters for a mode."""

            def __init__(self, default: type[flow.Actor]):
                self._default: type[flow.Actor] = default
                self._actor: typing.Optional[type[flow.Actor]] = None
                self._params: typing.Mapping[str, typing.Any] = {}

            def __call__(self, actor: typing.Optional[type[flow.Actor]] = None, /, **params: typing.Any) -> None:
                self._actor = actor or self._default
                self._params = params

            def spec(self, *args, **kwargs) -> typing.Optional[flow.Spec]:
                """Create the actor task.Spec from previously provided config or do nothing if no config provided.

                Args:
                    *args: Optional args for the Spec instance.
                    **kwargs: Optional kwargs for the Spec instance.

                Returns:
                    Spec instance or None if not configured.
                """
                if not self._actor:
                    return None
                return self._actor.spec(*args, **self._params | kwargs)

        train = property(lambda self: Adapter.Builder.Decorator(self, self._train))
        apply = property(lambda self: Adapter.Builder.Decorator(self, self._apply))
        label = property(lambda self: Adapter.Builder.Decorator(self, self._label))

        def __init__(self, actor: type[flow.Actor]):
            self._train: Adapter.Builder.Setter = self.Setter(actor)
            self._apply: Adapter.Builder.Setter = self.Setter(actor)
            self._label: Adapter.Builder.Setter = self.Setter(actor)

        def __call__(self, *args, **kwargs) -> 'Adapter':
            return Adapter(
                self._apply.spec(*args, **kwargs), self._train.spec(*args, **kwargs), self._label.spec(*args, **kwargs)
            )

    class Decorator:
        """Adapter-level decorator used to create Adapter (builder) instances in the first place."""

        def __init__(self, builder: property):
            self._builder: property = builder

        def __call__(
            self,
            actor: typing.Optional[typing.Union[type[flow.Actor], 'Adapter.Builder']] = None,
            /,
            **params: typing.Any,
        ) -> 'Adapter.Builder':
            """Actor decorator for creating curried operator that get instantiated upon another (optionally
            parametrized) call.

            Args:
                actor: Decorated actor class.
                **params: Optional operator kwargs.

            Returns:
                Curried operator.
            """

            def decorator(actor: typing.Union[type[flow.Actor], 'Adapter.Builder']) -> 'Adapter.Builder':
                """Decorating function."""
                if not isinstance(actor, Adapter.Builder):
                    actor = Adapter.Builder(actor)
                self._builder.fget(actor)(**params)
                return actor

            return decorator(actor) if actor else decorator

    def __init__(
        self,
        apply: typing.Optional[flow.Spec] = None,
        train: typing.Optional[flow.Spec] = None,
        label: typing.Optional[flow.Spec] = None,
    ):
        for mode in apply, train, label:
            if mode and mode.actor.is_stateful():
                raise forml.InvalidError('Stateful actor invalid for an adapter')
        self._apply: typing.Optional[flow.Spec] = apply
        self._train: typing.Optional[flow.Spec] = train
        self._label: typing.Optional[flow.Spec] = label

    def __repr__(self):
        return (
            f'{self.__class__.__name__}'
            f'[apply={repr(self._apply)}, train={repr(self._train)}, label={repr(self._label)}]'
        )

    train = staticmethod(Decorator(Builder.train))
    apply = staticmethod(Decorator(Builder.apply))
    label = staticmethod(Decorator(Builder.label))

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Composition implementation.

        Args:
            left: Left side track builder.

        Returns:
            Composed track.
        """

        def worker(mode: typing.Optional[flow.Spec]) -> typing.Optional[flow.Worker]:
            """Create a worker for given spec if not None.

            Args:
                mode: Task spec for given mode.

            Returns:
                Worker instance or None.
            """
            if mode:
                mode = flow.Worker(mode, 1, 1)
            return mode

        return left.expand().extend(worker(self._apply), worker(self._train), worker(self._label))
