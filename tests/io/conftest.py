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

import pytest

from forml.io import etl
from forml.io.dsl.schema import frame, kind


@pytest.fixture(scope='session')
def person() -> frame.Table:
    """Base table fixture.
    """

    class Person(etl.Schema):
        """Base table.
        """
        surname = etl.Field(kind.String())
        dob = etl.Field(kind.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: frame.Table) -> frame.Table:
    """Extended table fixture.
    """

    class Student(person):
        """Extended table.
        """
        level = etl.Field(kind.Integer())
        score = etl.Field(kind.Float())
        school = etl.Field(kind.Integer())

    return Student


@pytest.fixture(scope='session')
def school() -> frame.Table:
    """School table fixture.
    """

    class School(etl.Schema):
        """School table.
        """
        sid = etl.Field(kind.Integer(), 'id')
        name = etl.Field(kind.String())

    return School
