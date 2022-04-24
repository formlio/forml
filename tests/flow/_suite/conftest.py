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
Flow unit tests fixtures.
"""
# pylint: disable=no-self-use

import pytest

from forml import flow
from forml.io import layout


@pytest.fixture(scope='function')
def operator(actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Operator:
    """Operator fixture."""

    class Operator(flow.Operator):
        """Operator mock."""

        def compose(self, left: flow.Composable) -> flow.Trunk:
            """Dummy composition."""
            track = left.expand()
            trainer = flow.Worker(actor_spec, 1, 1)
            applier = trainer.fork()
            extractor = flow.Worker(actor_spec, 1, 1)
            trainer.train(track.train.publisher, extractor[0])
            return track.use(label=track.train.extend(extractor)).extend(applier)

    return Operator()


@pytest.fixture(scope='function')
def origin(actor_spec: flow.Spec[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]]) -> flow.Operator:
    """Origin operator fixture."""

    class Operator(flow.Operator):
        """Operator mock."""

        def compose(self, left: flow.Composable) -> flow.Trunk:
            """Dummy composition."""
            trainer = flow.Worker(actor_spec, 1, 1)
            applier = trainer.fork()
            return flow.Trunk(applier, trainer)

    return Operator()
