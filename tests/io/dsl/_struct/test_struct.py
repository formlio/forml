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
# pylint: disable=no-self-use
import cloudpickle

from forml.io import dsl


class TestField:
    """Field unit tests."""

    def test_renamed(self, student_table: dsl.Table):
        """Test the field renaming."""
        assert student_table.schema.dob.renamed('foo').name == 'foo'


def test_schema(student_table: dsl.Table):
    """Programmatic schema assembly test."""
    assert dsl.schema(*student_table.schema) == student_table.schema


class TestSchema:
    """Schema unit tests."""

    def test_serilizable(self, student_table: dsl.Table):
        """Test schema serializability."""
        assert cloudpickle.loads(cloudpickle.dumps(student_table.schema)) == student_table.schema
        assert cloudpickle.loads(cloudpickle.dumps(student_table)) == student_table
