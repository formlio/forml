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
Global ForML unit tests fixtures.
"""
# pylint: disable=no-self-use
import datetime

import pandas
import pytest

from forml.io import dsl


@pytest.fixture(scope='session')
def student_data(student: dsl.Table) -> pandas.DataFrame:
    """Student table fixture."""
    return pandas.DataFrame(
        [
            ['smith', datetime.date(2012, 8, 20), 3, 1.1, 1, datetime.datetime(2019, 4, 3)],
            ['brown', datetime.date(2014, 3, 11), 2, 1.4, 1, datetime.datetime(2019, 4, 5)],
            ['white', datetime.date(2017, 1, 1), 1, -3, 2, datetime.datetime(2019, 4, 1)],
        ],
        columns=(c.name for c in student.features),
    )


@pytest.fixture(scope='session')
def school_data(school: dsl.Table) -> pandas.DataFrame:
    """School table fixture."""

    return pandas.DataFrame([[1, 'prodigy'], [2, 'dummies']], columns=(c.name for c in school.features))
