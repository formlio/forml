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
Struct tests.
"""
import pickle
import typing

import numpy
import pytest

import forml
from forml.io import dsl, layout


class TestField:
    """Field unit tests."""

    def test_renamed(self, student_table: dsl.Table):
        """Test the field renaming."""
        assert student_table.schema.dob.renamed('foo').name == 'foo'


class TestSchema:
    """Schema unit tests."""

    def test_serilizable(self, student_table: dsl.Table):
        """Test schema serializability."""
        assert pickle.loads(pickle.dumps(student_table.schema)) == student_table.schema
        assert pickle.loads(pickle.dumps(student_table)) == student_table

    def test_from_fields(self, student_table: dsl.Table):
        """Test the programmatic schema assembly from fields."""
        assert dsl.Schema.from_fields(*student_table.schema) == student_table.schema

    @pytest.mark.parametrize(
        'record, names, fields',
        [
            ('foo', [], [dsl.Field(dsl.String(), name='c0')]),
            (1, ['bar'], [dsl.Field(dsl.Integer(), name='bar')]),
            (numpy.float64(0), [], [dsl.Field(dsl.Float(), name='c0')]),
            (['foo', 1], ['bar', 'baz'], [dsl.Field(dsl.String(), name='bar'), dsl.Field(dsl.Integer(), name='baz')]),
            (
                numpy.array([('foo', 1)], dtype='U21, int')[0],
                ['bar'],
                [dsl.Field(dsl.String(), name='bar'), dsl.Field(dsl.Integer(), name='c1')],
            ),
        ],
    )
    def test_from_record(self, record: layout.Native, names: typing.Sequence[str], fields: typing.Sequence[dsl.Any]):
        """Test the schema inference."""
        assert list(dsl.Schema.from_record(record, *names)) == fields  # pylint: disable=not-an-iterable

    def test_from_path(self, person_table: dsl.Table):
        """Test the schema importing."""
        module = person_table.schema.__module__
        assert dsl.Schema.from_path(f'{module}:{person_table.__class__.__qualname__}') == person_table
        with pytest.raises(forml.InvalidError, match='Not a schema path:'):
            dsl.Schema.from_path('foobar')
        with pytest.raises(forml.InvalidError, match='Not a schema:'):
            dsl.Schema.from_path(f'{module}:PACKAGE')
        with pytest.raises(forml.MissingError, match='No such schema:'):
            dsl.Schema.from_path(f'{module}:fobar')
        with pytest.raises(forml.MissingError, match='No such module:'):
            dsl.Schema.from_path('123foobar:baz')
