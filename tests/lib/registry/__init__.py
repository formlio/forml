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
# pylint: disable=no-self-use
import abc
import typing
import uuid

import pytest

from forml import project as prj
from forml.runtime.asset import persistent
from forml.runtime.asset.directory import generation as genmod
from forml.runtime.asset.directory import lineage as lngmod
from forml.runtime.asset.directory import project as prjmod


class Registry(metaclass=abc.ABCMeta):
    """Base class for registry unit tests."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='function')
    def constructor() -> typing.Callable[[], persistent.Registry]:
        """Registry fixture."""

    @staticmethod
    @pytest.fixture(scope='function')
    def empty(constructor: typing.Callable[[], persistent.Registry]) -> persistent.Registry:
        """Empty registry fixture."""
        return constructor()

    @staticmethod
    @pytest.fixture(scope='function')
    def populated(
        constructor: typing.Callable[[], persistent.Registry],
        project_package: prj.Package,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
        valid_generation: genmod.Level.Key,
        states: typing.Mapping[uuid.UUID, bytes],
        tag: genmod.Tag,
    ) -> persistent.Registry:
        """Populated registry fixture."""
        registry = constructor()
        registry.push(project_package)
        for sid, value in states.items():
            registry.write(project_name, project_lineage, sid, value)
        registry.close(project_name, project_lineage, valid_generation, tag)
        return registry

    def test_projects(self, empty: persistent.Registry, populated: persistent.Registry, project_name: prjmod.Level.Key):
        """Registry projects unit test."""
        assert not any(empty.projects())
        assert list(populated.projects()) == [project_name]

    def test_lineages(
        self,
        empty: persistent.Registry,
        populated: persistent.Registry,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
    ):
        """Registry lineages unit test."""
        assert not any(empty.lineages(project_name))
        assert list(populated.lineages(project_name)) == [project_lineage]

    def test_generations(
        self,
        empty: persistent.Registry,
        populated: persistent.Registry,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
        valid_generation: genmod.Level.Key,
    ):
        """Registry generations unit test."""
        assert not any(empty.lineages(project_name))
        assert list(populated.generations(project_name, project_lineage)) == [valid_generation]

    def test_push(self, empty: persistent.Registry, project_package: prj.Package):
        """Registry put unit test."""
        empty.push(project_package)

    def test_mount(
        self,
        populated: persistent.Registry,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
        project_package: prj.Package,
    ):
        """Registry take unit test."""
        assert populated.mount(project_name, project_lineage).package == project_package.manifest.package

    def test_read(
        self,
        populated: persistent.Registry,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
        valid_generation: genmod.Level.Key,
        states: typing.Mapping[uuid.UUID, bytes],
    ):
        """Registry load unit test."""
        for sid, value in states.items():
            assert populated.read(project_name, project_lineage, valid_generation, sid) == value

    def test_open(
        self,
        populated: persistent.Registry,
        project_name: prjmod.Level.Key,
        project_lineage: lngmod.Level.Key,
        valid_generation: genmod.Level.Key,
        tag: genmod.Tag,
    ):
        """Registry checkout unit test."""
        assert populated.open(project_name, project_lineage, valid_generation) == tag
