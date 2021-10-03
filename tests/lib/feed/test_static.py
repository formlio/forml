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
    def name() -> layout.Vector:
        """Feed column fixture."""
        return ['a', 'b', 'c']

    @staticmethod
    @pytest.fixture(scope='function')
    def age() -> layout.Vector:
        """Feed column fixture."""
        return [1, 2, 3]

    @staticmethod
    @pytest.fixture(scope='function')
    def data(name: layout.Vector, age: layout.Vector) -> layout.ColumnMajor:
        """Data fixture."""
        return [name, age]

    @staticmethod
    @pytest.fixture(scope='function')
    def feed(schema: dsl.Table, data: layout.ColumnMajor) -> static.Feed:
        """Feed fixture."""
        return static.Feed({schema: data})

    @staticmethod
    @pytest.fixture(scope='function')
    def reader(feed: static.Feed) -> typing.Callable[[dsl.Query], layout.ColumnMajor]:
        """Feed reader fixture."""
        return feed.reader(feed.sources, feed.features)

    def test_query(
        self,
        reader: typing.Callable[[dsl.Query], layout.ColumnMajor],
        schema: dsl.Table,
        name: layout.Vector,
        age: layout.Vector,
    ):
        """Test feed query."""
        assert reader(schema.select(schema.age, schema.name)) == [age, name]

    def test_unsupported(self, reader: typing.Callable[[dsl.Query], layout.ColumnMajor], schema: dsl.Table):
        """Test unsuported operations."""
        with pytest.raises(dsl.UnsupportedError):
            reader(schema.where(schema.age > 1))
        with pytest.raises(dsl.UnsupportedError):
            reader(schema.having(schema.age > 1))
        with pytest.raises(dsl.UnsupportedError):
            reader(schema.orderby(schema.name))
        with pytest.raises(dsl.UnsupportedError):
            reader(schema.limit(1))
        with pytest.raises(dsl.UnsupportedError):
            reader(schema.select(schema.age + 1))
