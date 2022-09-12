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
Feed ForML unit tests fixtures.
"""
import datetime

import pandas
import pytest

from forml.io import dsl


@pytest.fixture(scope='session')
def person_data(person_table: dsl.Table) -> pandas.DataFrame:
    """Person table fixture."""
    return pandas.DataFrame(
        [
            ['white', datetime.date(2017, 1, 1)],
            ['harris', datetime.date(2016, 8, 6)],
            ['black', datetime.date(2013, 7, 27)],
            ['smith', datetime.date(2015, 6, 19)],
            ['brown', datetime.date(2014, 3, 11)],
        ],
        columns=[c.name for c in person_table.features],
    )


@pytest.fixture(scope='session')
def student_data(person_data: pandas.DataFrame, student_table: dsl.Table) -> pandas.DataFrame:
    """Student table fixture."""
    extra = pandas.DataFrame(
        [
            [1, -3, 2, datetime.datetime(2019, 4, 4)],
            [3, 3.2, 3, datetime.datetime(2019, 4, 3)],
            [1, 2.3, 2, datetime.datetime(2019, 4, 2)],
            [1, 1.1, 1, datetime.datetime(2019, 4, 1)],
            [2, 0, 1, datetime.datetime(2019, 4, 5)],
        ],
    )
    data = pandas.concat([person_data, extra], axis='columns', ignore_index=True)
    data.columns = [c.name for c in student_table.features]
    return data


@pytest.fixture(scope='session')
def school_data(school_table: dsl.Table) -> pandas.DataFrame:
    """School table fixture."""

    return pandas.DataFrame(
        [[1, 'oxford'], [2, 'cambridge'], [3, 'stanford']], columns=[c.name for c in school_table.features]
    )
