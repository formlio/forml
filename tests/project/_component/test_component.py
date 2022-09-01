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
Project component tests.
"""
import typing

import pytest

import forml
from forml import flow, project, setup
from forml.io import dsl, layout
from forml.project import _component


def test_setup():
    """Test the direct setup access."""
    project.setup(object())


class TestVirtual:
    """Virtual component unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def component() -> typing.Any:
        """Component fixture."""
        return object()

    @staticmethod
    @pytest.fixture(scope='session', params=(None, 'foo', 'bar.baz'))
    def package(request) -> str:
        """Package fixture."""
        return request.param

    def test_load(self, component: typing.Any, package: str):
        """Test loading of the virtual component."""
        assert setup.load(_component.Virtual(component, package=package).path, project.setup) == component


class TestSource:
    """Source unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=('vector', 'table', 'actor', None))
    def labels(
        request,
        student_table: dsl.Table,
        actor_builder: flow.Builder[flow.Actor[layout.RowMajor, layout.Array, layout.RowMajor]],
    ) -> typing.Optional[project.Source.Labels]:
        """Component fixture."""
        if request.param == 'vector':
            return student_table.level
        if request.param == 'table':
            return student_table.level, student_table.level
        if request.param == 'actor':
            return actor_builder
        return None

    def test_invalid(self, student_table: dsl.Table, school_table: dsl.Table):
        """Test invalid query."""
        with pytest.raises(forml.InvalidError, match='Label-feature overlap'):
            project.Source.query(student_table, student_table.score)
        with pytest.raises(forml.InvalidError, match='Train-apply schema mismatch'):
            project.Source.query(student_table, apply=school_table)

    def test_query(self, source_query: dsl.Query, labels: typing.Optional[project.Source.Labels]):
        """Test the query setup."""
        query = project.Source.query(source_query, labels)
        assert isinstance(query.extract.train, dsl.Query)
        assert query.extract.apply == query.extract.train == source_query
        assert query.extract.labels == labels
