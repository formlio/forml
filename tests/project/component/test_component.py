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

from forml import error
from forml.io.dsl.struct import frame
from forml.project import component as compmod, importer


def test_setup():
    """Test the direct setup access."""
    compmod.setup(object())


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
        with importer.context(module):
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
        assert compmod.load(compmod.Virtual(component, package=package).path) == component


def test_load():
    """Testing the top level component.load() function."""
    provided = compmod.load('component', pathlib.Path(__file__).parent)
    import component  # pylint: disable=import-outside-toplevel

    assert provided is component.INSTANCE


class TestSource:
    """Source unit tests."""

    def test_query(self, schema: frame.Table):
        """Test the query setup."""
        with pytest.raises(error.Invalid):
            compmod.Source.query(schema, schema.age)
        query = compmod.Source.query(schema)
        assert isinstance(query.extract.train, frame.Query)
