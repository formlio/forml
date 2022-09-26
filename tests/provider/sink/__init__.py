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
Common sink implementations tests.
"""

import abc
import contextlib
import pickle
import typing

import pytest

from forml import io, runtime, setup
from forml.io import asset, layout

Context = typing.TypeVar('Context')


class Sink(abc.ABC):
    """Feed tests base class."""

    class Matcher(typing.Generic[Context], contextlib.AbstractContextManager):
        """Base matcher class."""

        def __init__(self, feed: io.Feed, instance: asset.Instance, sink: io.Sink):
            self._launcher: runtime.Launcher = runtime.Launcher(
                setup.Runner.resolve('dask'), instance, io.Importer(feed), io.Exporter(sink)
            )

        def __call__(self, expected: layout.Array) -> bool:
            with self as context:
                self._launcher.apply()
            return self.match(context, expected)

        def __exit__(self, exc_type, exc_val, exc_tb):
            return

        @abc.abstractmethod
        def match(self, context: Context, expected: layout.Array) -> bool:
            """Compare the matcher context with the expected value."""
            raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def sink() -> io.Sink:
        """Sink fixture."""

    @pytest.fixture(scope='function')
    def matcher(self, feed_instance: io.Feed, valid_instance: asset.Instance, sink: io.Sink) -> Matcher:
        """Sink launcher fixture."""
        return self.Matcher(feed_instance, valid_instance, sink)  # pylint: disable=abstract-class-instantiated

    def test_sink(self, matcher: Matcher, generation_prediction: layout.Array):
        """Test sink query."""
        assert matcher(generation_prediction)

    def test_serializable(self, sink: io.Sink):
        """Test sink serializability."""
        assert pickle.loads(pickle.dumps(sink)).__class__ == sink.__class__
