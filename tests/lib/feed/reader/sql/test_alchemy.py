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
SQL alchemy parser tests.
"""
# pylint: disable=no-self-use

import types
import typing

import pytest
from pyhive import sqlalchemy_trino as trino
from sqlalchemy import sql

from forml.io.dsl import parser as parsmod
from forml.io.dsl.struct import series, frame
from forml.lib.feed.reader.sql import alchemy
from . import Parser


class TestParser(Parser):
    """SQL parser unit tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(student: frame.Table, school: frame.Table) -> typing.Mapping[frame.Source, sql.Selectable]:
        """Sources mapping fixture."""
        return types.MappingProxyType({student: sql.table('student'), school: sql.table('school')})

    @staticmethod
    @pytest.fixture(scope='session')
    def columns(student: frame.Table) -> typing.Mapping[series.Column, sql.ColumnElement]:
        """Columns mapping fixture."""
        return types.MappingProxyType({student.level: sql.column('class')})

    @staticmethod
    @pytest.fixture(scope='session')
    def parser(
        sources: typing.Mapping[frame.Source, sql.Selectable], columns: typing.Mapping[series.Column, sql.ColumnElement]
    ) -> alchemy.Parser:
        """Parser fixture."""
        return alchemy.Parser(sources, columns)

    @staticmethod
    @pytest.fixture(scope='session')
    def formatter() -> typing.Callable[[parsmod.Source], str]:
        return lambda s: str(s.compile(dialect=trino.TrinoDialect(), compile_kwargs={'literal_binds': True}))

    class TestJoin(Parser.TestJoin):
        """SQL parser join unit test."""

        KIND = {
            None: 'JOIN',
            frame.Join.Kind.LEFT: 'LEFT OUTER JOIN',
            frame.Join.Kind.RIGHT: 'LEFT OUTER JOIN',
            frame.Join.Kind.FULL: 'FULL OUTER JOIN',
            frame.Join.Kind.INNER: 'JOIN',
            frame.Join.Kind.CROSS: 'FULL OUTER JOIN',
        }

        @classmethod
        def join(cls, kind: frame.Join.Kind) -> str:
            if kind != frame.Join.Kind.RIGHT:
                return f'"student" {cls.KIND[kind]} "school"'
            return f'"school" {cls.KIND[kind]} "student"'

        @staticmethod
        def condition(
            kind: frame.Join.Kind, student: frame.Table, school: frame.Table
        ) -> typing.Tuple[typing.Optional[series.Expression], str]:
            if kind != frame.Join.Kind.CROSS:
                return school.sid == student.school, ' ON "school"."id" = "student"."school"'
            return None, ' ON true'
