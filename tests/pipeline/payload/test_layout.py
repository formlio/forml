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
Layout unit tests.
"""
import pandas
import pytest

from forml.pipeline import payload


class TestFrame:
    """Frame layout unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def data() -> pandas.DataFrame:
        """DataFrame fixture."""
        return pandas.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})

    @staticmethod
    @pytest.fixture()
    def frame(data) -> payload.Frame:
        """Pandas frame fixture."""
        return payload.Frame(data)

    def test_rows(self, frame: payload.Frame, data: pandas.DataFrame):
        """Row operations tests."""
        rows = frame.to_rows()
        assert rows[0].equals(data.iloc[0])
        assert rows[:1][0].equals(data.iloc[0])
        assert frame.take_rows([0, 1]).to_rows()[0].equals(data.iloc[0])
        assert all((list(t) == list(f)) for t, (_, f) in zip(rows, data.iterrows()))

    def test_columns(self, frame: payload.Frame, data: pandas.DataFrame):
        """Column operations tests."""
        columns = frame.to_columns()
        assert columns[0].equals(data.iloc[:, 0])
        assert columns[:1][0].equals(data.iloc[:, 0])
        assert frame.take_columns([0, 1]).to_columns()[0].equals(data.iloc[:, 0])
        assert all((list(t) == list(f)) for t, (_, f) in zip(columns, data.items()))

    def test_hashable(self, frame: payload.Frame):
        """Hashable test."""
        assert hash(frame)
