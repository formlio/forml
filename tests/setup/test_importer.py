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
Project importer tests.
"""
import importlib
import os
import pathlib
import types

import pytest

import forml
from forml import project, setup


def test_isolated(project_package: project.Package, tmp_path: pathlib.Path):
    """Isolated importer unit test."""
    with pytest.raises(ModuleNotFoundError, match='No module'):
        setup.isolated(project_package.manifest.package, tmp_path)
    setup.isolated(project_package.manifest.package, project_package.path)
    setup.isolated(project_package.manifest.package, os.path.relpath(project_package.path, os.getcwd()))


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
        with setup.context(module):
            assert importlib.import_module(name) == module


def test_load():
    """Testing the top level ``load`` function."""
    path = pathlib.Path(__file__).parent

    with pytest.warns(match='unexpected component'):
        assert setup.load('component.unexpected', project.setup, path) is None

    provided = setup.load('component.valid', project.setup, path)
    from component import valid  # pylint: disable=import-outside-toplevel

    assert provided is valid.INSTANCE

    with pytest.raises(forml.UnexpectedError, match='Repeated call'):
        setup.load('component.repeated', project.setup, path)

    with pytest.raises(forml.InvalidError, match='incomplete'):
        setup.load('component.incomplete', project.setup, path)
