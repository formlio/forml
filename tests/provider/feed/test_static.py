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
Static feed unit tests.
"""
import pickle
import typing

import numpy
import pytest

from forml import io
from forml.io import dsl, layout
from forml.provider.feed import static


class TestFeed:
    """Feed unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def table(source_query: dsl.Query) -> dsl.Table:
        """Table fixture."""
        return dsl.Table(source_query.schema)

    @staticmethod
    @pytest.fixture(scope='function')
    def feed(table: dsl.Table, testset: layout.RowMajor) -> static.Feed:
        """Feed fixture."""
        return static.Feed({table: layout.Dense.from_rows(testset).to_columns()})

    @staticmethod
    @pytest.fixture(scope='function')
    def reader(feed: static.Feed) -> io.Feed.Reader:
        """Feed reader fixture."""
        return feed.producer(feed.sources, feed.features)

    def test_query(self, reader: io.Feed.Reader, table: dsl.Table, testset: layout.RowMajor):
        """Test feed query."""
        assert numpy.array_equal(reader(table.query).to_rows(), testset)

    def test_unsupported(self, reader: io.Feed.Reader, table: dsl.Table):
        """Test unsupported operations."""
        with pytest.raises(dsl.UnsupportedError):
            reader(table.where(table.score > 1))
        with pytest.raises(dsl.UnsupportedError):
            reader(table.having(table.score > 1))
        with pytest.raises(dsl.UnsupportedError):
            reader(table.orderby(table.surname))
        with pytest.raises(dsl.UnsupportedError):
            reader(table.limit(1))
        with pytest.raises(dsl.UnsupportedError):
            reader(table.select(table.score + 1))

    def test_serializable(self, feed: static.Feed):
        """Test instance serializability."""

        def flatten(features: typing.Mapping[dsl.Feature, layout.Array]) -> typing.Mapping[dsl.Feature, tuple]:
            """Helper for flattening the features."""
            return {f: tuple(c) for f, c in features.items()}

        # pylint: disable=protected-access
        assert flatten(pickle.loads(pickle.dumps(feed))._features) == flatten(feed._features)
