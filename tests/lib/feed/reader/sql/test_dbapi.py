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
SQL parser tests.
"""
# pylint: disable=no-self-use
import types
import typing

import pytest

from forml.io.dsl.struct import series, frame
from forml.lib.feed.reader.sql import dbapi
from . import Parser, Scenario, Case


class TestParser(Parser):
    """SQL parser unit tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(student: frame.Table, school: frame.Table) -> typing.Mapping[frame.Source, str]:
        """Sources mapping fixture."""
        return types.MappingProxyType({student: 'student', school: 'school'})

    @staticmethod
    @pytest.fixture(scope='session')
    def columns(student: frame.Table) -> typing.Mapping[series.Column, str]:
        """Columns mapping fixture."""
        return types.MappingProxyType({student.level: 'class'})

    @staticmethod
    @pytest.fixture(scope='session')
    def parser(sources: typing.Mapping[frame.Source, str], columns: typing.Mapping[series.Column, str]) -> dbapi.Parser:
        """Parser fixture."""
        return dbapi.Parser(sources, columns)

    class TestSubquery(Scenario):
        """SQL parser subquery unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student: frame.Table, school: frame.Table) -> Case:
            query = frame.Query(student.select(student.surname, student.score)).select(student.surname)
            expected = 'SELECT "student"."surname" FROM (SELECT "student"."surname", "student"."score" FROM "student")'
            return Case(query, expected)
