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
# pylint: disable=no-self-use

import typing

import pytest

from forml.io import dsl, layout
from forml.lib.feed import static


class TestFeed:
    """Feed unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def table(query: dsl.Query) -> dsl.Table:
        """Table fixture."""
        return dsl.Table(query.schema)

    @staticmethod
    @pytest.fixture(scope='function')
    def feed(table: dsl.Table, testset: layout.ColumnMajor) -> static.Feed:
        """Feed fixture."""
        return static.Feed({table: testset})

    @staticmethod
    @pytest.fixture(scope='function')
    def reader(feed: static.Feed) -> typing.Callable[[dsl.Query], layout.ColumnMajor]:
        """Feed reader fixture."""
        return feed.reader(feed.sources, feed.features)

    def test_query(
        self, reader: typing.Callable[[dsl.Query], layout.ColumnMajor], table: dsl.Table, testset: layout.ColumnMajor
    ):
        """Test feed query."""
        assert reader(table.query) == testset

    def test_unsupported(self, reader: typing.Callable[[dsl.Query], layout.ColumnMajor], table: dsl.Table):
        """Test unsuported operations."""
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
