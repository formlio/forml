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
Feed utils unit tests.
"""

import pytest

from forml import io, setup


class TestExporter:
    """Sink handle unit tests."""

    class Conf(setup.Sink):
        """Fake override of the setup.Feed class to bypass parsing config file."""

        def __new__(cls, reference: str, identity: str):
            return tuple.__new__(cls, [reference, {'identity': identity}])

    @pytest.fixture(scope='session')
    def modal(self, sink_reference: str) -> io.Exporter:
        """Sink.Mode based handle fixture."""
        apply = self.Conf(sink_reference, 'apply')
        eval_ = self.Conf(sink_reference, 'eval')
        return io.Exporter(setup.Sink.Mode([apply, eval_]))

    @pytest.fixture(scope='session')
    def instant(self, sink_type: type[io.Sink]) -> io.Exporter:
        """Instant based handle fixture."""
        return io.Exporter(sink_type(identity='instant'))

    def test_getter(self, modal: io.Exporter, instant: io.Exporter):
        """Test the handle getters."""
        assert modal.apply.identity == 'apply'
        assert modal.eval.identity == 'eval'
        assert instant.apply.identity == 'instant'
        assert instant.eval.identity == 'instant'
