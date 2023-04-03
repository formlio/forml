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
Swiss feed unit tests.
"""
import pathlib

import pandas
import pytest

from forml import io
from forml.io import dsl
from forml.provider.feed import monolite

from . import Feed


class TestFeed(Feed):
    """Feed unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source_dir(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
        """Session-wide temp dir for holding the feed sources."""
        return tmp_path_factory.mktemp('monolite-source')

    @staticmethod
    @pytest.fixture(scope='session')
    def school_csv(source_dir: pathlib.Path, school_data: pandas.DataFrame) -> pathlib.Path:
        """School data in a CSV file fixture."""
        path = source_dir / 'school.csv'
        school_data.to_csv(path, index=False)
        return path

    @staticmethod
    @pytest.fixture(scope='session')
    def student_parquet(source_dir: pathlib.Path, student_data: pandas.DataFrame) -> pathlib.Path:
        """School data in a Parquet file fixture."""
        path = source_dir / 'student.parquet'
        student_data.to_parquet(path, index=False)
        return path

    @staticmethod
    @pytest.fixture(scope='session')
    def feed(
        person_table: dsl.Table,
        person_data: pandas.DataFrame,
        student_table: dsl.Table,
        student_parquet: pathlib.Path,
        school_table: dsl.Table,
        school_csv: pathlib.Path,
    ) -> io.Feed:
        """Feed fixture."""
        return monolite.Feed(
            inline={person_table: person_data}, csv={school_table: school_csv}, parquet={student_table: student_parquet}
        )
