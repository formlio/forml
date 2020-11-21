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
import abc
import typing

from forml import provider as provmod
from forml.conf.parsed import provider as provcfg
from forml.flow import pipeline
from forml.flow.pipeline import topology
from forml.io import payload
from forml.io.sink import publish


class Provider(provmod.Interface, default=provcfg.Sink.default, path=provcfg.Sink.path):
    """Sink is an implementation of a specific data consumer."""

    class Writer(publish.Writer, metaclass=abc.ABCMeta):
        """Abstract sink writer."""

    def __init__(self, **writerkw):
        self._writerkw: typing.Dict[str, typing.Any] = writerkw

    def publish(self) -> pipeline.Segment:
        """Provide a pipeline composable segment implementing the publish action.

        Returns:
            Pipeline segment.
        """
        publisher: topology.Composable = publish.Operator(publish.Writer.Actor.spec(self.writer(**self._writerkw)))
        return publisher.expand()

    @classmethod
    def writer(cls, **kwargs: typing.Any) -> typing.Callable[[payload.ColumnMajor], None]:
        """Return the reader instance of this feed (any callable, presumably extract.Reader).

        Args:
            kwargs: Optional writer keyword arguments.

        Returns:
            Writer instance.
        """
        return cls.Writer(**kwargs)  # pylint: disable=abstract-class-instantiated


class Handle:
    """Sink handle is a lazy wrapper around alternative sink specifiers providing a particular Sink instance upon
    request.
    """

    def __init__(self, sink: typing.Union[provcfg.Sink.Mode, str, Provider]):
        if isinstance(sink, str):
            sink = provcfg.Sink.Mode.resolve(sink)
        self._sink: typing.Union[provcfg.Sink.Mode, Provider] = sink

    def __call__(self, getter: property) -> 'Provider':
        if isinstance(self._sink, Provider):  # already a Sink instance
            return self._sink
        assert isinstance(self._sink, provcfg.Sink.Mode)
        descriptor: provcfg.Sink = getter.fget(self._sink)
        return Provider[descriptor.reference](**descriptor.params)

    # pylint: disable=no-member
    train = property(lambda self: self(provcfg.Sink.Mode.train))
    apply = property(lambda self: self(provcfg.Sink.Mode.apply))
    eval = property(lambda self: self(provcfg.Sink.Mode.eval))
