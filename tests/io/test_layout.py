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
Payload tests.
"""
import pickle
import typing

import numpy
import pytest

from forml.io import layout


class TestDense:
    """Dense layout unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=([[1, 2, 'a'], [4, 5, 'b'], [7, 8, 'c']], ['x', 0, None], []))
    def rows(request) -> layout.Array:
        """Rows fixture."""
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def columns(rows: layout.Array) -> layout.Array:
        """Rows fixture."""
        return rows if not rows or not isinstance(rows[0], list) else [list(c) for c in zip(*rows)]

    @staticmethod
    @pytest.fixture()
    def table(rows: layout.Array) -> layout.Dense:
        """Dense table fixture."""
        return layout.Dense.from_rows(rows)

    def test_rows(self, table: layout.Dense, rows: layout.Array):
        """Row operations tests."""
        assert table.to_rows().tolist() == rows
        assert numpy.array_equal(layout.Dense.from_rows(rows).to_rows(), table.to_rows())
        if rows:
            assert table.take_rows([0]).to_rows().tolist() == [rows[0]]

    def test_columns(self, table: layout.Dense, columns: numpy.ndarray):
        """Column operations tests."""
        assert table.to_columns().tolist() == columns
        assert numpy.array_equal(layout.Dense.from_columns(columns).to_columns(), table.to_columns())
        if columns:
            assert table.take_columns([0]).to_columns().tolist() == [columns[0]]


class TestEncoding:
    """Encoding tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def encoding() -> layout.Encoding:
        """Encoding fixture."""
        return layout.Encoding('application/json', foo='1', bar='2')

    @pytest.mark.parametrize(
        'raw, kind, options',
        [
            ('application/json; charset=UTF-8', ['application/json'], [{'charset': 'UTF-8'}]),
            (
                'image/GIF; q=0.6; a=x, image/jpeg; q=0.6, text/html; q=1.0, text/*; q=0.8, image/*; q=0.5, */*; q=0.1',
                ['text/html', 'text/*', 'image/gif', 'image/jpeg', 'image/*', '*/*'],
                [{}, {}, {'a': 'x'}, {}, {}, {}],
            ),
        ],
    )
    def test_parse(self, raw: str, kind: typing.Sequence[str], options: typing.Sequence[typing.Mapping[str, str]]):
        """Encoding parsing test."""
        assert all(e == (k, o) for e, k, o in zip(layout.Encoding.parse(raw), kind, options))

    @pytest.mark.parametrize(
        'pattern, subject, matches',
        [
            ('application/json', 'application/json', True),
            ('application/json; foo=1; bar=2', 'application/json; bar=2; foo=1', True),
            ('application/json; foo=1; bar=2', 'application/json; bar=2', False),
            ('application/json', 'Application/JSON', True),
            ('application/json', 'foobar/json', False),
            ('application/*', 'application/json', True),
            ('*/*', 'application/json', True),
            ('*/*; foo=1', 'application/json', False),
            ('*/*', '*/json', False),
        ],
    )
    def test_match(self, pattern: str, subject: str, matches: bool):
        """Encoding matching test."""
        assert layout.Encoding.parse(pattern)[0].match(layout.Encoding.parse(subject)[0]) == matches

    def test_header(self, encoding: layout.Encoding):
        """Encoding header test."""
        assert layout.Encoding.parse(encoding.header)[0] == encoding

    def test_hashable(self, encoding: layout.Encoding):
        """Encoding hashability test."""
        assert hash(encoding)

    def test_serializable(self, encoding: layout.Encoding):
        """Encoding serializability test."""
        assert pickle.loads(pickle.dumps(encoding)) == encoding


class TestRequest:
    """Request unit tests."""

    def test_serializable(self, testset_request: layout.Request):
        """Request serializability test."""
        assert pickle.loads(pickle.dumps(testset_request)) == testset_request
