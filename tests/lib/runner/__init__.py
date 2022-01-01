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
Common runner implementations tests.
"""
# pylint: disable=no-self-use

import abc
import multiprocessing

import pytest

from forml import io
from forml.io import layout
from forml.runtime import asset, facility


class Runner(abc.ABC):
    """Runner tests base class."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='function')
    def runner(valid_instance: asset.Instance, feed_instance: io.Feed, sink_instance: io.Sink) -> facility.Runner:
        """Runner fixture."""

    def test_apply(
        self, runner: facility.Runner, sink_output: multiprocessing.Queue, generation_prediction: layout.Array
    ):
        """Test runner apply mode."""
        runner.apply()
        assert tuple(sink_output.get_nowait()) == generation_prediction

    def test_train(self, runner: facility.Runner):
        """Test runner train mode."""
        runner.train()
