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
Stdout sink tests.
"""
import contextlib
import io as stdio
import multiprocessing
import typing

import pytest

from forml import io
from forml.io import asset, dsl, layout
from forml.provider.sink import stdout

from . import Sink


class TestSink(Sink):
    """Stdout sink tests."""

    class Matcher(Sink.Matcher[None]):
        """Stdout sink matcher."""

        class Sink(io.Sink):
            """Sink wrapper with capturing consumer."""

            def __init__(self, sink: io.Sink, queue: multiprocessing.Queue):
                super().__init__(queue=queue, sink=sink)

            @classmethod
            def consumer(cls, schema: typing.Optional[dsl.Source.Schema], **kwargs: typing.Any) -> io.Consumer:
                """Wrapped consumer factory."""

                def consumer(data: layout.RowMajor) -> layout.Outcome:
                    """Stdout capturing consumer."""
                    with contextlib.redirect_stdout(stdio.StringIO()) as output:
                        outcome = sink.consumer(schema, **kwargs)(data)
                    queue.put_nowait(output.getvalue())
                    return outcome

                queue = kwargs.pop('queue')
                sink = kwargs.pop('sink')
                return consumer

        def __init__(self, feed: io.Feed, instance: asset.Instance, sink: io.Sink):
            self._manager = multiprocessing.Manager()
            self._queue = self._manager.Queue()
            self._value = None
            super().__init__(feed, instance, self.Sink(sink, self._queue))

        def __enter__(self):
            self._manager.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._value = self._queue.get_nowait()
            self._manager.__exit__(exc_type, exc_val, exc_tb)

        def match(self, context: None, expected: layout.Array) -> bool:  # pylint: disable=unused-argument
            """Capture the standard output and compare it with the expected value."""
            with contextlib.redirect_stdout(stdio.StringIO()) as output:
                print(list(expected))
            return self._value == output.getvalue()

    @staticmethod
    @pytest.fixture(scope='session')
    def sink() -> io.Sink:
        """Sink fixture."""
        return stdout.Sink()
