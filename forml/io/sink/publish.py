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

"""Publish utilities.
"""

import abc
import logging
import typing

from forml import error
from forml.flow import task, pipeline
from forml.flow.graph import node
from forml.flow.pipeline import topology
from forml.io import payload

LOGGER = logging.getLogger(__name__)


class Operator(topology.Operator):
    """Basic publisher operator."""

    def __init__(self, writer: task.Spec):
        if writer.actor.is_stateful():
            raise error.Invalid('Stateful actor invalid for a publisher')
        self._writer: task.Spec = writer

    def compose(self, left: topology.Composable) -> pipeline.Segment:
        """Compose the publisher segment track.

        Returns:
            Sink segment track.
        """
        apply: node.Worker = node.Worker(self._writer, 1, 0)
        train: node.Worker = apply.fork()
        return left.expand().extend(apply, train)


class Writer(typing.Generic[payload.Native], metaclass=abc.ABCMeta):
    """Base class for writer implementation."""

    class Actor(task.Actor):
        """Data publishing actor using the provided writer to store the data."""

        def __init__(self, writer: typing.Callable[[payload.ColumnMajor], None]):
            self._writer: typing.Callable[[payload.ColumnMajor], None] = writer

        def __repr__(self):
            return repr(self._writer)

        def apply(self, data: payload.ColumnMajor) -> None:
            self._writer(data)

    def __init__(self, **kwargs: typing.Any):
        self._kwargs: typing.Mapping[str, typing.Any] = kwargs

    def __repr__(self):
        return task.name(self.__class__, **self._kwargs)

    def __call__(self, data: payload.ColumnMajor) -> None:
        LOGGER.debug('Starting to publish')
        return self.write(self.format(data), **self._kwargs)

    @classmethod
    def format(cls, data: payload.ColumnMajor) -> payload.Native:
        """Format the output data into the required payload.Native format.

        Args:
            data: Output data.

        Returns:
            Data formatted into payload.Native format.
        """
        return data

    @classmethod
    @abc.abstractmethod
    def write(cls, data: payload.Native, **kwargs: typing.Any) -> None:
        """Perform the write operation with the given data.

        Args:
            data: Output data in the writer's native format.
            kwargs: Optional writer keyword args.
        """
