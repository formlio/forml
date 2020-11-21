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
Set of generic operator skeletons that can be simply used as wrappers about relevant actors.
"""
import abc
import typing

from forml import error
from forml.flow import task, pipeline
from forml.flow.graph import node
from forml.flow.pipeline import topology


class Base(topology.Operator, metaclass=abc.ABCMeta):
    """Simple is a generic single actor operator."""

    SZIN = 1
    SZOUT = 1

    def __init__(self, spec: task.Spec):
        self.spec: task.Spec = spec

    def __repr__(self):
        return f'{self.__class__.__name__}[{repr(self.spec)}]'

    @classmethod
    def operator(
        cls, actor: typing.Optional[typing.Type[task.Actor]] = None, /, **params
    ) -> typing.Callable[..., 'Base']:
        """Actor decorator for creating curried operator that get instantiated upon another (optionally parametrized)
        call.

        Args:
            actor: Decorated actor class.
            **params: Optional operator kwargs.

        Returns:
            Curried operator.
        """

        def decorator(actor: typing.Type[task.Actor]) -> typing.Callable[..., Base]:
            """Decorating function."""

            def simple(*args, **kwargs) -> Base:
                """Curried operator.

                Args:
                    **kwargs: Operator params.

                Returns:
                    Operator instance.
                """
                return cls(task.Spec(actor, *args, **{**params, **kwargs}))

            return simple

        if actor:
            decorator = decorator(actor)
        return decorator

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Abstract composition implementation.

        Args:
            left: Left side track builder.

        Returns:
            Composed track.
        """
        return self.apply(node.Worker(self.spec, self.SZIN, self.SZOUT), left.expand())

    @abc.abstractmethod
    def apply(self, applier: node.Worker, left: pipeline.Segment) -> pipeline.Segment:
        """Apply functionality to be implemented by child.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns:
            Composed segment track.
        """


class Mapper(Base):
    """Basic transformation operator with one input and one output port for each mode."""

    def apply(self, applier: node.Worker, left: pipeline.Segment) -> pipeline.Segment:
        """Mapper composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns:
            Composed segment track.
        """
        train_applier: node.Worker = applier.fork()
        if self.spec.actor.is_stateful():
            train_trainer: node.Worker = applier.fork()
            train_trainer.train(left.train.publisher, left.label.publisher)
        return left.extend(applier, train_applier)


class Consumer(Base):
    """Basic operator with one input and one output port in apply mode and no output in train mode."""

    def __init__(self, spec: task.Spec):
        if not spec.actor.is_stateful():
            raise error.Invalid('Stateless actor invalid for a consumer')
        super().__init__(spec)

    def apply(self, applier: node.Worker, left: pipeline.Segment) -> pipeline.Segment:
        """Consumer composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns:
            Composed segment track.
        """
        trainer: node.Worker = applier.fork()
        trainer.train(left.train.publisher, left.label.publisher)
        return left.extend(applier)


class Labeler(Base):
    """Basic label extraction operator.

    Provider actor is expected to have shape of (1, 2) where first output port is a train and second is label.
    """

    SZOUT = 2

    def apply(self, applier: node.Worker, left: pipeline.Segment) -> pipeline.Segment:
        """Labeler composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns:
            Composed segment track.
        """
        train: node.Future = node.Future()
        label: node.Future = node.Future()
        train[0].subscribe(applier[0])
        label[0].subscribe(applier[1])
        applier[0].subscribe(left.train.publisher)
        return left.use(train=left.train.extend(tail=train), label=left.train.extend(tail=label))
