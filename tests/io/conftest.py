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

import typing

import pytest

from forml.io import feed as feedmod, sink as sinkmod
from forml.io.dsl import function, parser, struct
from forml.io.dsl.struct import frame, kind


@pytest.fixture(scope='session')
def person() -> frame.Table:
    """Base table fixture."""

    class Person(struct.Schema):
        """Base table."""

        surname = struct.Field(kind.String())
        dob = struct.Field(kind.Date(), 'birthday')

    return Person


@pytest.fixture(scope='session')
def student(person: frame.Table) -> frame.Table:
    """Extended table fixture."""

    class Student(person):
        """Extended table."""

        level = struct.Field(kind.Integer())
        score = struct.Field(kind.Float())
        school = struct.Field(kind.Integer())

    return Student


@pytest.fixture(scope='session')
def school() -> frame.Table:
    """School table fixture."""

    class School(struct.Schema):
        """School table."""

        sid = struct.Field(kind.Integer(), 'id')
        name = struct.Field(kind.String())

    return School


@pytest.fixture(scope='session')
def school_ref(school: frame.Table) -> frame.Reference:
    """School table reference fixture."""
    return school.reference('bar')


@pytest.fixture(scope='session')
def query(person: frame.Table, student: frame.Table, school_ref: frame.Reference) -> frame.Query:
    """Query fixture."""
    query = (
        student.join(person, student.surname == person.surname)
        .join(school_ref, student.school == school_ref.sid)
        .select(student.surname.alias('student'), school_ref['name'], function.Cast(student.score, kind.String()))
        .where(student.score < 2)
        .orderby(student.level, student.score)
        .limit(10)
    )
    return query


@pytest.fixture(scope='session')
def reference() -> str:
    """Dummy feed/sink reference fixture"""
    return 'dummy'


@pytest.fixture(scope='session')
def feed(
    reference: str, person: frame.Table, student: frame.Table, school: frame.Table  # pylint: disable=unused-argument
) -> typing.Type[feedmod.Provider]:
    """Dummy feed fixture."""

    class Dummy(feedmod.Provider, alias=reference):
        """Dummy feed for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

        @property
        def sources(self) -> typing.Mapping[frame.Source, parser.Source]:
            """Abstract method implementation."""
            return {student.join(person, student.surname == person.surname).source: None, student: None, school: None}

    return Dummy


@pytest.fixture(scope='session')
def sink(reference: str) -> typing.Type[sinkmod.Provider]:  # pylint: disable=unused-argument
    """Dummy sink fixture."""

    class Dummy(sinkmod.Provider, alias=reference):
        """Dummy sink for unit-testing purposes."""

        def __init__(self, identity: str, **readerkw):
            super().__init__(**readerkw)
            self.identity: str = identity

    return Dummy
