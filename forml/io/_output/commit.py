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

import logging
import typing

import forml
from forml import flow
from forml.io import layout

LOGGER = logging.getLogger(__name__)


class Operator(flow.Operator):
    """Basic publisher operator."""

    def __init__(self, writer: flow.Spec[flow.Actor[layout.RowMajor, None, layout.Native]]):
        if writer.actor.is_stateful():
            raise forml.InvalidError('Stateful actor invalid for a publisher')
        self._writer: flow.Spec[flow.Actor[layout.RowMajor, None, layout.Native]] = writer

    def compose(self, left: flow.Composable) -> flow.Trunk:
        """Compose the publisher segment track.

        Returns:
            Sink segment track.
        """
        apply: flow.Worker = flow.Worker(self._writer, 1, 0)
        train: flow.Worker = apply.fork()
        return left.expand().extend(apply, train)


Consumer = typing.Callable[[layout.RowMajor], layout.Outcome]


class Driver(flow.Actor[layout.RowMajor, None, layout.Outcome]):
    """Data publishing actor using the provided writer to store the data."""

    def __init__(self, consumer: Consumer):
        self._consumer: Consumer = consumer

    def __repr__(self):
        return repr(self._consumer)

    def apply(self, data: layout.RowMajor) -> layout.Outcome:
        return self._consumer(data)
