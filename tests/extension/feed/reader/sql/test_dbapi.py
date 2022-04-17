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

from forml.extension.feed.reader.sql import dbapi
from forml.io import dsl

from . import Case, Parser, Scenario


class TestParser(Parser):
    """SQL parser unit tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(student_table: dsl.Table, school_table: dsl.Table) -> typing.Mapping[dsl.Source, str]:
        """Sources mapping fixture."""
        return types.MappingProxyType({student_table: 'student', school_table: 'school'})

    @staticmethod
    @pytest.fixture(scope='session')
    def features(student_table: dsl.Table) -> typing.Mapping[dsl.Feature, str]:
        """Columns mapping fixture."""
        return types.MappingProxyType({student_table.level: 'class'})

    @staticmethod
    @pytest.fixture(scope='session')
    def parser(sources: typing.Mapping[dsl.Source, str], features: typing.Mapping[dsl.Feature, str]) -> dbapi.Parser:
        """Parser fixture."""
        return dbapi.Parser(sources, features)

    class TestSubquery(Scenario):
        """SQL parser subquery unit test."""

        @staticmethod
        @pytest.fixture(scope='session')
        def case(student_table: dsl.Table, school_table: dsl.Table) -> Case:
            query = dsl.Query(student_table.select(student_table.surname, student_table.score)).select(
                student_table.surname
            )
            expected = 'SELECT "student"."surname" FROM (SELECT "student"."surname", "student"."score" FROM "student")'
            return Case(query, expected)
