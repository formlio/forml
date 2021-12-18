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
# pylint: disable=no-self-use
import importlib
import pathlib
import types
import typing

import pytest

import forml
from forml import project
from forml.io import dsl
from forml.project import _component, _importer


def test_setup():
    """Test the direct setup access."""
    project.setup(object())


class TestContext:
    """Context unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def name() -> str:
        """Module name fixture."""
        return 'foo'

    @staticmethod
    @pytest.fixture(scope='session')
    def module(name: str) -> types.ModuleType:
        """Module fixture."""
        return types.ModuleType(name)

    def test_context(self, name: str, module: types.ModuleType):
        """Testing the context manager."""
        with _importer.context(module):
            assert importlib.import_module(name) == module


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
        assert _component.load(_component.Virtual(component, package=package).path) == component


def test_load():
    """Testing the top level component.load() function."""
    provided = _component.load('component', pathlib.Path(__file__).parent)
    import component  # pylint: disable=import-outside-toplevel

    assert provided is component.INSTANCE


class TestSource:
    """Source unit tests."""

    def test_query(self, student: dsl.Table):
        """Test the query setup."""
        with pytest.raises(forml.InvalidError):
            project.Source.query(student, student.score)
        query = project.Source.query(student)
        assert isinstance(query.extract.train, dsl.Query)
