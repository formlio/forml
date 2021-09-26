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
# pylint: disable=no-self-use

import pytest

from forml import error, io
from forml.conf.parsed import provider as conf
from forml.io import dsl


class TestImporter:
    """Feed pool unit tests."""

    class Conf(conf.Feed):
        """Fake override of the conf.Feed class to bypass parsing config file."""

        def __new__(cls, reference: str, priority: float, identity: str):
            return tuple.__new__(cls, [reference, priority, {'identity': identity}])

    def test_iter(self, feed: type[io.Feed], reference: str):
        """Test the pool iterator."""
        conf10 = self.Conf(reference, 10, 'conf10')
        conf1000 = self.Conf(reference, 1000, 'conf1000')
        instant = feed(identity='instant')
        pool = io.Importer(conf10, instant, conf1000)
        assert tuple(f.identity for f in pool) == ('instant', 'conf1000', 'conf10')

    def test_match(self, feed: type[io.Feed], query: dsl.Query, person: dsl.Table):
        """Feed matching test."""
        instance = feed(identity='instance')
        pool = io.Importer(instance)
        assert pool.match(query) is instance
        with pytest.raises(error.Missing):
            pool.match(person)
