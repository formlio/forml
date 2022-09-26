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
Common feed implementations tests.
"""

import abc
import pickle

import numpy
import pytest

from forml import io, project, runtime
from forml.io import layout
from forml.pipeline import payload


class Feed(abc.ABC):
    """Feed tests base class."""

    class Launcher:
        """Feed test launcher."""

        def __init__(self, feed: io.Feed, source: project.Source):
            self._handler: runtime.Virtual.Handler = source.bind(payload.Sniff()).launcher(runner='dask', feeds=[feed])

        @property
        def apply(self) -> numpy.array:
            """Apply-mode result."""
            return numpy.array(self._handler.apply(), dtype=object)

        @property
        def train(self) -> tuple[numpy.array, numpy.array]:
            """Train-mode result."""
            result = self._handler.train()
            return numpy.array(result.features, dtype=object), numpy.array(result.labels, dtype=object)

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def feed() -> io.Feed:
        """Feed fixture."""

    @pytest.fixture(scope='session')
    def launcher(self, feed: io.Feed, project_components: project.Components) -> Launcher:
        """Feed fixture."""
        return self.Launcher(feed, project_components.source)

    def test_apply(self, launcher: Launcher, testset: layout.RowMajor):
        """Test feed apply-mode query."""
        assert numpy.array_equal(testset, launcher.apply)

    def test_train(self, launcher: Launcher, trainset_features: layout.RowMajor, trainset_labels: layout.Array):
        """Test feed train-mode query."""
        features, labels = launcher.train
        assert numpy.array_equal(trainset_features, features)
        assert numpy.array_equal(trainset_labels, labels)

    def test_serializable(self, feed: io.Feed):
        """Test feed serializability."""
        assert pickle.loads(pickle.dumps(feed)).__class__ == feed.__class__
