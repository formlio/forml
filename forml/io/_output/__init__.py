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
IO sink utils.
"""
import typing

from forml import flow, provider, setup

from . import _consumer, commit

if typing.TYPE_CHECKING:
    from forml import io
    from forml.io import dsl


class Sink(provider.Service, default=setup.Sink.default, path=setup.Sink.path):
    """Abstract base class for pipeline output sink providers.

    It integrates the concept of a ``Writer`` provided using the :meth:`consumer` method
    or by overriding the inner ``.Writer`` class.
    """

    Writer = _consumer.Writer

    def __init__(self, **writerkw):
        self._writerkw: dict[str, typing.Any] = writerkw

    def save(self, schema: typing.Optional['dsl.Source.Schema']) -> flow.Composable:
        """Provide a pipeline composable implementing the publish action.

        Returns:
            Pipeline composable.
        """
        return commit.Operator(commit.Driver.builder(self.consumer(schema, **self._writerkw)))

    @classmethod
    def consumer(cls, schema: typing.Optional['dsl.Source.Schema'], **kwargs: typing.Any) -> 'io.Consumer':
        """Consumer factory method.

        A ``Consumer`` is a generic callable interface most typically represented using the
        :class:`forml.io.Sink.Writer` implementation whose task is to commit the pipeline output
        using an external media layer.

        Unless overloaded, the method returns an instance of ``cls.Writer`` (which might be easier
        to extend without needing to overload this method).

        Note:
            For compatibility with the :doc:`serving mode <serving>`, the callable ``Consumer`` is
            (contra-intuitively) expected to provide a return value.

        Args:
            schema: Result schema.
            kwargs: Optional writer keyword arguments.

        Returns:
            Consumer instance.
        """
        return cls.Writer(schema, **kwargs)  # pylint: disable=abstract-class-instantiated


class Exporter:
    """Sink exporter is a lazy wrapper around alternative sink specifiers providing a particular
    Sink instance upon request.
    """

    def __init__(self, sink: typing.Union[setup.Sink.Mode, str, Sink]):
        if isinstance(sink, str):
            sink = setup.Sink.Mode.resolve(sink)
        self._sink: typing.Union[setup.Sink.Mode, Sink] = sink

    def __call__(self, getter: property) -> 'Sink':
        if isinstance(self._sink, Sink):  # already a Sink instance
            return self._sink
        assert isinstance(self._sink, setup.Sink.Mode)
        descriptor: setup.Sink = getter.fget(self._sink)
        return Sink[descriptor.reference](**descriptor.params)

    # pylint: disable=no-member
    train = property(lambda self: self(setup.Sink.Mode.train))
    apply = property(lambda self: self(setup.Sink.Mode.apply))
    eval = property(lambda self: self(setup.Sink.Mode.eval))
