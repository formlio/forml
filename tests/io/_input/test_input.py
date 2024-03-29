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

import forml
from forml import io, setup
from forml.io import dsl


class TestImporter:
    """Feed pool unit tests."""

    class Conf(setup.Feed):
        """Fake override of the setup.Feed class to bypass parsing config file."""

        def __new__(cls, reference: str, priority: float, identity: str):
            return tuple.__new__(cls, [reference, priority, {'identity': identity}])

    def test_iter(self, feed_type: type[io.Feed], feed_reference: str):
        """Test the pool iterator."""
        conf10 = self.Conf(feed_reference, 10, 'conf10')
        conf1000 = self.Conf(feed_reference, 1000, 'conf1000')
        instant = feed_type(identity='instant')
        pool = io.Importer(conf10, instant, conf1000)
        assert tuple(f.identity for f in pool) == ('instant', 'conf1000', 'conf10')

    def test_match(self, feed_type: type[io.Feed], source_query: dsl.Query):
        """Feed matching test."""
        instance = feed_type(identity='instance')
        pool = io.Importer(instance)
        assert pool.match(source_query) is instance
        with pytest.raises(forml.MissingError):
            pool.match(dsl.Table(source_query.schema))
