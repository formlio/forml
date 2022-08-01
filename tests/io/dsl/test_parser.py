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
ETL code statement tests.
"""
import types
import typing

import pytest

from forml.io import dsl
from forml.io.dsl import function
from forml.io.dsl import parser as parsmod


class TestContainer:
    """Parser stack unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def storage() -> parsmod.Container:
        """Stack fixture."""
        return parsmod.Container()

    @staticmethod
    @pytest.fixture(scope='function')
    def value() -> object:
        """Value fixture."""
        return object()

    def test_context(self, storage, value):
        """Test context nesting."""
        with storage:
            storage.context.symbols.push(value)
            with storage:
                with pytest.raises(RuntimeError):
                    storage.context.symbols.pop()
            assert storage.fetch() == value
        with pytest.raises(RuntimeError):
            with storage:
                storage.context.symbols.push(value)


class Parser(parsmod.Visitor[tuple, tuple]):  # pylint: disable=unsubscriptable-object
    """Dummy frame parser wrapping all terms into tuples."""

    # pylint: disable=missing-function-docstring
    def generate_element(self, origin: tuple, element: tuple) -> tuple:
        return origin, element

    def generate_literal(self, value: typing.Any, kind: dsl.Any) -> tuple:
        return value, kind

    def generate_expression(self, expression: type[dsl.Expression], arguments: typing.Sequence[typing.Any]) -> tuple:
        return expression, *arguments

    def generate_alias(self, feature: tuple, alias: str) -> tuple:
        return feature, alias

    # pylint: disable=missing-function-docstring
    def generate_join(self, left: tuple, right: tuple, condition: tuple, kind: dsl.Join.Kind) -> tuple:
        return left, kind, right, condition

    def generate_set(self, left: tuple, right: tuple, kind: dsl.Set.Kind) -> tuple:
        return left, kind, right

    def generate_query(
        self,
        source: tuple,
        features: typing.Sequence[tuple],
        where: typing.Optional[tuple],
        groupby: typing.Sequence[tuple],
        having: typing.Optional[tuple],
        orderby: typing.Sequence[tuple[tuple, dsl.Ordering.Direction]],
        rows: typing.Optional[dsl.Rows],
    ) -> tuple:
        return source, tuple(features), where, tuple(groupby), having, tuple(orderby), rows

    def generate_reference(self, instance: tuple, name: str) -> tuple:
        return (instance, name), (name,)


class TestParser:
    """Frame parser tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sources(
        person_table: dsl.Table, student_table: dsl.Table, school_table: dsl.Table
    ) -> typing.Mapping[dsl.Source, tuple]:
        """Sources mapping fixture."""
        return types.MappingProxyType(
            {
                student_table.inner_join(person_table, student_table.surname == person_table.surname): tuple(['foo']),
                person_table: tuple([person_table]),
                student_table: tuple([student_table]),
                school_table: tuple([school_table]),
            }
        )

    @staticmethod
    @pytest.fixture(scope='session')
    def features(student_table: dsl.Table) -> typing.Mapping[dsl.Feature, tuple]:
        """Features mapping fixture."""

        class Features:
            """Features mapping."""

            def __getitem__(self, feature: dsl.Feature) -> tuple:
                if feature == student_table.level:
                    return tuple(['baz'])
                if isinstance(feature, dsl.Element):
                    return tuple([feature])
                raise KeyError('Unknown feature')

        return Features()

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(sources: typing.Mapping[dsl.Source, tuple], features: typing.Mapping[dsl.Feature, tuple]) -> Parser:
        """Parser fixture."""
        return Parser(sources, features)

    @pytest.fixture(scope='session')
    def school_ref(self, school_table: dsl.Table) -> dsl.Reference:
        """School table reference fixture."""
        return school_table.reference('bar')

    def test_parsing(
        self,
        source_query: dsl.Query,
        student_table: dsl.Table,
        school_ref: dsl.Reference,
        parser: parsmod.Visitor,
    ):
        """Parsing test."""
        with parser:
            source_query.accept(parser)
            result = parser.fetch()
        assert result[0][0] == ('foo',)
        assert result[1] == (
            ((student_table,), (student_table.surname,)),
            ((('bar',), (school_ref['name'],)), 'school'),
            ((function.Cast, ((student_table,), (student_table.score,)), dsl.Integer()), 'score'),
        )
        assert result[5] == (
            (((student_table,), (student_table.updated,)), dsl.Ordering.Direction.ASCENDING),
            (((student_table,), (student_table.surname,)), dsl.Ordering.Direction.ASCENDING),
        )
