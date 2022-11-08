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
import functools
import typing

from forml import flow as flowmod

if typing.TYPE_CHECKING:
    from forml import flow  # pylint: disable=reimported
    from forml.pipeline import wrap


class Decorator:
    """Helper for implementing the decorator functions."""

    def __init__(self, setup: typing.Callable[[type['wrap.Operator'], 'flow.Builder'], 'Setup']):
        self._setup: typing.Callable[[type['wrap.Operator'], 'flow.Builder'], Setup] = setup

    def __call__(
        self,
        parent: type['wrap.Operator'],
        inner: typing.Optional[typing.Union[type['flow.Actor'], type['wrap.Operator']]] = None,
        /,
        **kwargs: typing.Any,
    ) -> typing.Callable[..., 'wrap.Operator']:
        def decorator(inner: typing.Union[type['flow.Actor'], type['wrap.Operator']]) -> 'wrap.Operator':
            """Decorating function."""
            if issubclass(inner, flowmod.Actor):
                builder = inner.builder(**kwargs)
            else:
                nonlocal parent
                parent = inner
                builder = inner.Default.reset(**kwargs)
            operator = Meta(inner.__name__, (), {}, setup=self._setup(parent, builder))
            return functools.update_wrapper(operator, inner, updated=())

        return decorator(inner) if inner else decorator


class Operator(flowmod.Operator, metaclass=abc.ABCMeta):
    """Special operator created via a decoration of particular actors.

    This represents a convenient way of implementing ForML *Operators* without requiring to fully
    implement the :class:`flow.Operator <forml.flow.Operator>` base class from scratch.

    Attention:
        Instances are expected to be created via the decorator methods.

    This approach is applicable only to a special case of *simple* operators implemented by at most
    one actor per each of the coherent :ref:`appy/train/label segments <topology-coherence>`
    corresponding to the relevant *primitive* decorators (:meth:`apply`, :meth:`train`,
    :meth:`label`) supplying the particular actors.

    In addition to the primitive decorators, there is the combined :meth:`mapper` decorator filling
    both the train/apply segments at once.

    Hint:
        The decorators can be *chained* together as well as applied in a *split* fashion
        onto separate actors for different builder::

            @wrap.Operator.train
            @wrap.Operator.apply  # can be chained if same actor is also to be used in another mode
            @wrap.Actor.apply
            def MyOperator(df, *, myarg=None):
                ... # stateless actor implementation used for train/apply segments

            @MyOperator.label  # decorated operator can itself be used as decorator in split fashion
            @wrap.Actor.apply
            def MyOperator(df, *, myarg=None):
                ... # stateless actor implementation used for the label segment

    .. rubric:: Decorator Methods

    Actor definitions for individual builder can be provided using the following decorator methods.

    Methods:
        train(actor):
            Train segment actor decorator.

            When used as a decorator, this method creates an *operator* engaging the wrapped *actor*
            in the *train-mode*. If *stateful*, the actor also gets normally trained first. Note it
            does not get applied to the *apply-mode* features unless also decorated with the
            :meth:`apply` decorator (this is rarely desired - see the :meth:`mapper` decorator for
            a more typical use case)!

            Parameters:
                actor: Decorated actor.

            Returns:
                An Operator class using the given actor.

            Examples:
                Usage with a wrapped *stateless* actor::

                    @wrap.Operator.train
                    @wrap.Actor.apply
                    def TrainOnlyDropColumn(
                        df: pandas.DataFrame, *, column: str
                    ) -> pandas.DataFrame:
                        return df.drop(columns=column)

                    PIPELINE = AnotherOperator() >> TrainOnlyDropColumn(column='foo')

        apply(actor):
            Apply segment actor decorator.

            When used as a decorator, this method creates an *operator* engaging the wrapped *actor*
            in the *apply-mode*. If *stateful*, the actor also gets normally trained in *train-mode*
            (but does not get applied to the train-mode features unless also decorated with the
            :meth:`train` decorator!).

            Parameters:
                actor: Decorated actor.

            Returns:
                An Operator class using the given actor.

            Examples:
                Usage with a wrapped *stateful* actor::

                    @wrap.Actor.train
                    def ApplyOnlyFillnaMean(
                        state: typing.Optional[float],
                        df: pandas.DataFrame,
                        labels: pandas.Series,
                        *,
                        column: str,
                    ) -> float:
                        return df[column].mean()

                    @wrap.Operator.apply
                    @ApplyOnlyFillnaMean.apply
                    def ApplyOnlyFillnaMean(
                        state: float,
                        df: pandas.DataFrame,
                        *,
                        column: str
                    ) -> pandas.DataFrame:
                        df[column] = df[column].fillna(state)
                        return df

                    PIPELINE = (
                        AnotherOperator()
                        >> TrainOnlyDropColumn(column='foo')
                        >> ApplyOnlyFillnaMean(column='bar')
                    )

        label(actor):
            Label segment actor decorator.

            When used as a decorator, this method creates an *operator* engaging the wrapped *actor*
            in the *train-mode* as the *label transformer*. If *stateful*, the actor also gets
            normally trained first. The actor gets engaged prior to any other stateful actors
            potentially added to the same operator (using the :meth:`train` or :meth:`apply`
            decorators).

            Parameters:
                actor: Decorated actor.

            Returns:
                An Operator class using the given actor.

            Examples:
                Usage with a wrapped *stateless* actor::

                    @wrap.Operator.label
                    @wrap.Actor.apply
                    def LabelOnlyFillZero(labels: pandas.Series) -> pandas.Series:
                        return labels.fillna(0)

                    PIPELINE = (
                        anotheroperator()
                        >> LabelOnlyFillZero()
                        >> TrainOnlyDropColumn(column='foo')
                        >> ApplyOnlyFillnaMean(column='bar')
                    )

                Alternatively, it could as well be just added to the existing
                ``ApplyOnlyFillnaMean``::

                    @ApplyOnlyFillnaMean.label
                    @wrap.Actor.apply
                    def ApplyFillnaMeanLabelFillZero(labels: pandas.Series) -> pandas.Series:
                        return labels.fillna(0)

        mapper(actor):
            Combined train-apply decorator.

            Decorator representing the wrapping of the same actor using both the :meth:`train`
            and :meth:`apply` decorators effectively engaging the actor in transforming the
            features in both the *train-mode* as well as the *apply-mode*.

            Parameters:
                actor: Decorated actor.

            Returns:
                An Operator class using the given actor.
    """

    @property
    @abc.abstractmethod
    def Default(self) -> 'flow.Builder':  # pylint: disable=invalid-name
        """Builder provided in scope of the inner decorator (to be injected by metaclass)."""

    @property
    def Apply(self) -> typing.Optional['flow.Builder']:  # pylint: disable=invalid-name
        """Apply path actor builder (to be injected by metaclass)."""
        return None

    @property
    def Train(self) -> typing.Optional['flow.Builder']:  # pylint: disable=invalid-name
        """Train path actor builder (to be injected by metaclass)."""
        return None

    @property
    def Label(self) -> typing.Optional['flow.Builder']:  # pylint: disable=invalid-name
        """Label path actor builder (to be injected by metaclass)."""
        return None

    apply = classmethod(Decorator(lambda parent, builder: Setup(builder, builder, parent.Train, parent.Label)))
    train = classmethod(Decorator(lambda parent, builder: Setup(builder, parent.Apply, builder, parent.Label)))
    label = classmethod(Decorator(lambda parent, builder: Setup(builder, parent.Apply, parent.Train, builder)))
    mapper = classmethod(Decorator(lambda parent, builder: Setup(builder, builder, builder, parent.Label)))

    def __init__(self, *args, **kwargs):
        self._args: tuple[typing.Any] = args
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def compose(self, scope: 'flow.Composable') -> 'flow.Trunk':
        """Composition implementation.

        Args:
            scope: Left side composition builder.

        Returns:
            Composed trunk.
        """
        left = scope.expand()
        apply = train = label = None
        label_publisher = left.label.publisher
        if self.Label:
            label = flowmod.Worker(self.Label.update(*self._args, **self._kwargs), 1, 1)
            if self.Label.actor.is_stateful():
                label.fork().train(left.train.publisher, left.label.publisher)
            label_publisher = label[0]
        if self.Apply:
            apply = flowmod.Worker(self.Apply.update(*self._args, **self._kwargs), 1, 1)
            if self.Apply.actor.is_stateful():
                apply.fork().train(left.train.publisher, label_publisher)
        if self.Train:
            if self.Train == self.Apply:
                train = apply.fork()
            else:
                train = flowmod.Worker(self.Train.update(*self._args, **self._kwargs), 1, 1)
                if self.Train.actor.is_stateful():
                    train.fork().train(left.train.publisher, label_publisher)

        return left.extend(apply, train, label)


class Setup(typing.NamedTuple):
    """Combo of the individual actor builders."""

    default: 'flow.Builder' = Operator.Default
    apply: typing.Optional['flow.Builder'] = Operator.Apply
    train: typing.Optional['flow.Builder'] = Operator.Train
    label: typing.Optional['flow.Builder'] = Operator.Label


class Meta(abc.ABCMeta):
    """Metaclass for dynamically creating the decorated operator classes."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type],
        namespace: dict[str, typing.Any],
        setup: Setup = Setup(),  # noqa: B008
    ):
        return super().__new__(
            mcs,
            name,
            (Operator,),
            {
                Operator.Default.fget.__name__: setup.default,
                Operator.Apply.fget.__name__: setup.apply,
                Operator.Train.fget.__name__: setup.train,
                Operator.Label.fget.__name__: setup.label,
            },
        )
