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
ForML persistent unit tests.
"""
import abc
import pickle
import typing
import uuid

import pytest

from forml import project as prj
from forml.io import asset


class Registry(metaclass=abc.ABCMeta):
    """Base class for registry unit tests."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def constructor() -> typing.Callable[[], asset.Registry]:
        """Registry fixture."""

    @staticmethod
    @pytest.fixture(scope='function')
    def empty(constructor: typing.Callable[[], asset.Registry]) -> asset.Registry:
        """Empty registry fixture."""
        return constructor()

    @staticmethod
    @pytest.fixture(scope='function')
    def populated(
        constructor: typing.Callable[[], asset.Registry],
        project_package: prj.Package,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
        generation_states: typing.Mapping[uuid.UUID, bytes],
        generation_tag: asset.Tag,
    ) -> asset.Registry:
        """Populated registry fixture."""
        registry = constructor()
        registry.push(project_package)
        for sid, value in generation_states.items():
            registry.write(project_name, project_release, sid, value)
        registry.close(project_name, project_release, valid_generation, generation_tag)
        return registry

    def test_projects(self, empty: asset.Registry, populated: asset.Registry, project_name: asset.Project.Key):
        """Registry projects unit test."""
        assert not any(empty.projects())
        assert list(populated.projects()) == [project_name]

    def test_releases(
        self,
        populated: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
    ):
        """Registry releases unit test."""
        assert list(populated.releases(project_name)) == [project_release]

    def test_generations(
        self,
        populated: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
    ):
        """Registry generations unit test."""
        assert list(populated.generations(project_name, project_release)) == [valid_generation]

    def test_push(self, empty: asset.Registry, project_package: prj.Package):
        """Registry put unit test."""
        empty.push(project_package)

    def test_mount(
        self,
        populated: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        project_package: prj.Package,
    ):
        """Registry take unit test."""
        assert populated.mount(project_name, project_release).package == project_package.manifest.package

    def test_read(
        self,
        populated: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
        generation_states: typing.Mapping[uuid.UUID, bytes],
    ):
        """Registry load unit test."""
        for sid, value in generation_states.items():
            assert populated.read(project_name, project_release, valid_generation, sid) == value

    def test_open(
        self,
        populated: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
        generation_tag: asset.Tag,
    ):
        """Registry checkout unit test."""
        assert populated.open(project_name, project_release, valid_generation) == generation_tag

    def test_serializable(self, populated: asset.Registry):
        """Test registry serializability."""
        assert pickle.loads(pickle.dumps(populated)) == populated
