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

from forml import flow, provider
from forml.conf.parsed import provider as provcfg

from .. import dsl
from . import _consumer, commit


class Sink(provider.Service, default=provcfg.Sink.default, path=provcfg.Sink.path):
    """Sink is an implementation of a specific data consumer."""

    Writer = _consumer.Writer

    def __init__(self, **writerkw):
        self._writerkw: dict[str, typing.Any] = writerkw

    def save(self, schema: typing.Optional[dsl.Source.Schema]) -> flow.Trunk:
        """Provide a pipeline composable segment implementing the publish action.

        Returns:
            Pipeline segment.
        """
        publisher: flow.Composable = commit.Operator(commit.Driver.spec(self.consumer(schema, **self._writerkw)))
        return publisher.expand()

    @classmethod
    def consumer(cls, schema: typing.Optional[dsl.Source.Schema], **kwargs: typing.Any) -> commit.Consumer:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            schema: Product schema.
            kwargs: Optional writer keyword arguments.

        Returns:
            Consumer instance.
        """
        return cls.Writer(schema, **kwargs)  # pylint: disable=abstract-class-instantiated


class Exporter:
    """Sink exporter is a lazy wrapper around alternative sink specifiers providing a particular Sink instance upon
    request.
    """

    def __init__(self, sink: typing.Union[provcfg.Sink.Mode, str, Sink]):
        if isinstance(sink, str):
            sink = provcfg.Sink.Mode.resolve(sink)
        self._sink: typing.Union[provcfg.Sink.Mode, Sink] = sink

    def __call__(self, getter: property) -> 'Sink':
        if isinstance(self._sink, Sink):  # already a Sink instance
            return self._sink
        assert isinstance(self._sink, provcfg.Sink.Mode)
        descriptor: provcfg.Sink = getter.fget(self._sink)
        return Sink[descriptor.reference](**descriptor.params)

    # pylint: disable=no-member
    train = property(lambda self: self(provcfg.Sink.Mode.train))
    apply = property(lambda self: self(provcfg.Sink.Mode.apply))
    eval = property(lambda self: self(provcfg.Sink.Mode.eval))
