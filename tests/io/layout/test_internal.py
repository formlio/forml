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
Internal payload tests.
"""

import numpy
import pandas
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

    def test_hashable(self, table: layout.Dense):
        """Hashable test."""
        assert hash(table)


class TestFrame:
    """Frame layout unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def data() -> pandas.DataFrame:
        """DataFrame fixture."""
        return pandas.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

    @staticmethod
    @pytest.fixture()
    def frame(data) -> layout.Frame:
        """Pandas frame fixture."""
        return layout.Frame(data)

    def test_rows(self, frame: layout.Frame, data: pandas.DataFrame):
        """Row operations tests."""
        rows = frame.to_rows()
        assert rows[0].equals(data.iloc[0])
        assert rows[:1][0].equals(data.iloc[0])
        assert frame.take_rows([0, 1]).to_rows()[0].equals(data.iloc[0])
        assert all((list(t) == list(f)) for t, (_, f) in zip(rows, data.iterrows()))

    def test_columns(self, frame: layout.Frame, data: pandas.DataFrame):
        """Column operations tests."""
        columns = frame.to_columns()
        assert columns[0].equals(data.iloc[:, 0])
        assert columns[:1][0].equals(data.iloc[:, 0])
        assert frame.take_columns([0, 1]).to_columns()[0].equals(data.iloc[:, 0])
        assert all((list(t) == list(f)) for t, (_, f) in zip(columns, data.items()))

    def test_hashable(self, frame: layout.Frame):
        """Hashable test."""
        assert hash(frame)
