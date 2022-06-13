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

from forml import flow


class Decorator:
    """Decorator facility for multi-level decoration."""

    class Builder:
        """Operator builder carrying the parameters during decorations."""

        class Decorator:
            """Builder-level decorator used for overriding specific modes on previously decorated instances."""

            def __init__(self, builder: 'Decorator.Builder', setter: 'Decorator.Builder.Setter'):
                self._builder: Decorator.Builder = builder
                self._setter: Decorator.Builder.Setter = setter

            def __call__(
                self, actor: typing.Optional[type[flow.Actor]] = None, /, **params: typing.Any
            ) -> typing.Union['Decorator.Builder', typing.Callable[[type[flow.Actor]], 'Decorator.Builder']]:
                def decorator(actor: type[flow.Actor]) -> 'Decorator.Builder':
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

            def builder(self, *args, **kwargs) -> typing.Optional[flow.Builder]:
                """Create the actor builder from previously provided config or do nothing if no config provided.

                Args:
                    *args: Optional args for the Builder instance.
                    **kwargs: Optional kwargs for the Builder instance.

                Returns:
                    Builder instance or None if not configured.
                """
                if not self._actor:
                    return None
                return self._actor.builder(*args, **self._params | kwargs)

        train = property(lambda self: self.Decorator(self, self._train))
        apply = property(lambda self: self.Decorator(self, self._apply))
        label = property(lambda self: self.Decorator(self, self._label))

        def __init__(self, actor: type[flow.Actor]):
            self._train: Decorator.Builder.Setter = self.Setter(actor)
            self._apply: Decorator.Builder.Setter = self.Setter(actor)
            self._label: Decorator.Builder.Setter = self.Setter(actor)

        def __call__(self, *args, **kwargs) -> 'Operator':
            return Operator(
                self._apply.builder(*args, **kwargs),
                self._train.builder(*args, **kwargs),
                self._label.builder(*args, **kwargs),
            )

    def __init__(self, builder: property):
        self._builder: property = builder

    def __call__(
        self,
        actor: typing.Optional[typing.Union[type[flow.Actor], 'Decorator.Builder']] = None,
        /,
        **params: typing.Any,
    ) -> 'Decorator.Builder':
        """Actor decorator for creating curried operator that get instantiated upon another (optionally
        parametrized) call.

        Args:
            actor: Decorated actor class.
            **params: Optional operator kwargs.

        Returns:
            Decorated operator.
        """

        def decorator(actor: typing.Union[type[flow.Actor], 'Decorator.Builder']) -> 'Decorator.Builder':
            """Decorating function."""
            if not isinstance(actor, Decorator.Builder):
                actor = Decorator.Builder(actor)
            self._builder.fget(actor)(**params)
            return actor

        return decorator(actor) if actor else decorator


class Operator(flow.Operator, metaclass=abc.ABCMeta):
    """Wrapper for creating operators from simple actors.

    Actor definitions for individual modes can be provided using convenient decorators:

        @wrap.Operator.train(**kwargs)  # optional kwargs will be passed to actor
        @wrap.Operator.apply  # decorators can be chained if same actor is supposed to be used for another mode
        @wrap.Actor.apply
        def myadapter(df, **kwargs):
            # stateless actor implementation used for train/apply segments

        @myadapter.label(**kwargs)  # previously decorated adapter can be itself used as decorator
        @wrap.Actor.apply
        def myadapter(df, **kwargs):
            # stateless actor implementation used for label segment
    """

    def __init__(
        self,
        apply: typing.Optional[flow.Builder] = None,
        train: typing.Optional[flow.Builder] = None,
        label: typing.Optional[flow.Builder] = None,
    ):
        if label and label.actor.is_stateful():
            raise TypeError('Stateful actor invalid for a Label transformer')
        self._apply: typing.Optional[flow.Builder] = apply
        self._train: typing.Optional[flow.Builder] = train
        self._label: typing.Optional[flow.Builder] = label

    def __repr__(self):
        return (
            f'{self.__class__.__name__}'
            f'[apply={repr(self._apply)}, train={repr(self._train)}, label={repr(self._label)}]'
        )

    train = staticmethod(Decorator(Decorator.Builder.train))
    """Train segment decorator."""
    apply = staticmethod(Decorator(Decorator.Builder.apply))
    """Apply segment decorator."""
    label = staticmethod(Decorator(Decorator.Builder.label))
    """Label segment decorator."""

    @classmethod
    def mapper(
        cls,
        actor: typing.Optional[type[flow.Actor]] = None,
        /,
        **params: typing.Any,
    ) -> typing.Callable[..., 'Operator']:
        """Extra decorator for actor to be used for both the train and apply segment."""

        def decorator(actor: type[flow.Actor]) -> typing.Callable[..., Operator]:
            """Decorating function."""

            def operator(*args, **kwargs) -> Operator:
                """Decorated operator.

                Args:
                    **kwargs: Operator params.

                Returns:
                    Operator instance.
                """
                builder = flow.Builder(actor, *args, **params | kwargs)
                return cls(apply=builder, train=builder)

            return operator

        return decorator(actor) if actor else decorator

    def compose(self, scope: flow.Composable) -> flow.Trunk:
        """Composition implementation.

        Args:
            scope: Left side composition builder.

        Returns:
            Composed trunk.
        """
        left = scope.expand()
        apply = train = label = None
        label_publisher = left.label.publisher
        if self._label:
            label = flow.Worker(self._label, 1, 1)
            label_publisher = label[0]
        if self._apply:
            apply = flow.Worker(self._apply, 1, 1)
            if self._apply.actor.is_stateful():
                apply.fork().train(left.train.publisher, label_publisher)
        if self._train:
            if self._train == self._apply:
                train = apply.fork()
            else:
                train = flow.Worker(self._train, 1, 1)
                if self._train.actor.is_stateful():
                    train.fork().train(left.train.publisher, label_publisher)

        return left.extend(apply, train, label)
