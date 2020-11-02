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

from forml.flow import pipeline
from forml.flow import task
from forml.flow.graph import node
from forml.flow.pipeline import topology


@pytest.fixture(scope='function')
def operator(spec: task.Spec) -> topology.Operator:
    """Operator fixture.
    """
    class Operator(topology.Operator):
        """Operator mock.
        """
        def compose(self, left: topology.Composable) -> pipeline.Segment:
            """Dummy composition.
            """
            track = left.expand()
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            extractor = node.Worker(spec, 1, 1)
            trainer.train(track.train.publisher, extractor[0])
            return track.use(label=track.train.extend(extractor)).extend(applier)

    return Operator()


@pytest.fixture(scope='function')
def origin(spec: task.Spec) -> topology.Operator:
    """Origin operator fixture.
    """
    class Operator(topology.Operator):
        """Operator mock.
        """
        def compose(self, left: topology.Composable) -> pipeline.Segment:
            """Dummy composition.
            """
            trainer = node.Worker(spec, 1, 1)
            applier = trainer.fork()
            return pipeline.Segment(applier, trainer)

    return Operator()
