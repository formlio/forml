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
Project steuptools tests.
"""
import pathlib
import typing

import pytest
import setuptools
import toml

import forml
from forml import project
from forml.project import _setuptools


class TestDistribution:
    """Distribution unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def distribution(project_tree: project.Tree) -> _setuptools.Distribution:
        """Test project distribution fixture."""

        return setuptools.setup(
            tree=project_tree,
            distclass=_setuptools.Distribution,
            script_args=['--version'],
        )

    def test_artifact(self, distribution: _setuptools.Distribution, project_artifact: project.Artifact):
        """Test the artifact loaded using this distribution."""
        assert distribution.artifact == project_artifact


class TestTree:
    """Project tree unit tests."""

    @pytest.mark.parametrize(
        'content, name, version, dependencies, package, components',
        [
            (
                {
                    'project': {
                        'name': 'foobar',
                        'version': '1.2.3',
                        'dependencies': ['foo', 'bar'],
                    },
                    'tool': {'forml': {'package': 'foo.bar', 'components': {'source': 'foo.bar.input'}}},
                },
                'foobar',
                '1.2.3',
                ('foo', 'bar'),
                'foo.bar',
                {'source': 'foo.bar.input'},
            ),
            (
                {},
                project.Tree.DEFAULT_NAME,
                project.Tree.DEFAULT_VERSION,
                (),
                project.Tree.DEFAULT_NAME,
                {},
            ),
        ],
    )
    def test_parse(
        self,
        content: typing.Mapping[str, typing.Any],
        name: str,
        version: str,
        dependencies: typing.Sequence[str],
        package: str,
        components: typing.Mapping[str, typing.Any],
        tmp_path: pathlib.Path,
    ):
        """Test valid project tree parsing."""
        with open(tmp_path / 'pyproject.toml', 'w', encoding='utf-8') as pyproject:
            toml.dump(content, pyproject)
        tree = project.Tree(tmp_path)
        assert tree.name == name
        assert str(tree.version) == version
        assert tree.dependencies == dependencies
        assert tree.package == package
        assert tree.components == components

    def test_invalid(self, tmp_path: pathlib.Path):
        """Test invalid project tree parsing."""
        with pytest.raises(forml.MissingError, match='Invalid ForML project'):
            _ = project.Tree(tmp_path).name
        with open(tmp_path / 'pyproject.toml', 'w', encoding='utf-8') as pyproject:
            pyproject.write('invalid')
        with pytest.raises(forml.InvalidError, match='Invalid ForML project'):
            _ = project.Tree(tmp_path).name
