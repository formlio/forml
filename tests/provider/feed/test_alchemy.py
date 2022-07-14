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
Alchemy feed unit tests.
"""

import pandas
import pytest
import sqlalchemy

from forml import io
from forml.io import dsl
from forml.provider.feed import alchemy

from . import Feed


class TestFeed(Feed):
    """Feed unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def dburl(
        tmp_path_factory: pytest.TempPathFactory,
        person_data: pandas.DataFrame,
        student_data: pandas.DataFrame,
        school_data: pandas.DataFrame,
    ) -> str:
        """SQLite DB URL fixture."""
        path = tmp_path_factory.mktemp('alchemy') / 'test.db'
        url = f'sqlite:///{path.absolute()}'
        connection = sqlalchemy.create_engine(url)
        person_data.to_sql('person', connection, index=False)
        student_data.to_sql('student', connection, index=False)
        school_data.to_sql('school', connection, index=False)
        return url

    @staticmethod
    @pytest.fixture(scope='session')
    def feed(person_table: dsl.Table, student_table: dsl.Table, school_table: dsl.Table, dburl: str) -> io.Feed:
        """Feed fixture."""
        sources = {
            f'{student_table.schema.__module__}:{student_table.__class__.__qualname__}': 'student',
            school_table: 'school',
            person_table: 'person',
        }
        return alchemy.Feed(sources=sources, connection=dburl)
