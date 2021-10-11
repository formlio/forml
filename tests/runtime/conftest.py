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
Runtime unit tests fixtures.
"""
# pylint: disable=no-self-use
import typing
import uuid

import pytest

from forml import project as prj
from forml.runtime import asset


@pytest.fixture(scope='session')
def populated_lineage(project_lineage: asset.Lineage.Key) -> asset.Lineage.Key:
    """Lineage fixture."""
    return project_lineage


@pytest.fixture(scope='session')
def empty_lineage() -> asset.Lineage.Key:
    """Lineage fixture."""
    return asset.Lineage.Key('2')


@pytest.fixture(scope='session')
def last_lineage(empty_lineage: asset.Lineage.Key) -> asset.Lineage.Key:
    """Lineage fixture."""
    return empty_lineage


@pytest.fixture(scope='session')
def last_generation(valid_generation: asset.Generation.Key) -> asset.Generation.Key:
    """Generation fixture."""
    return valid_generation


@pytest.fixture(scope='function')
def registry(
    project_name: asset.Project.Key,
    populated_lineage: asset.Lineage.Key,
    empty_lineage: asset.Lineage.Key,
    valid_generation: asset.Generation.Key,
    tag: asset.Tag,
    states: typing.Mapping[uuid.UUID, bytes],
    project_package: prj.Package,
) -> asset.Registry:
    """Registry fixture."""
    content = {
        project_name: {
            populated_lineage: (project_package, {valid_generation: (tag, tuple(states.values()))}),
            empty_lineage: (project_package, {}),
        }
    }

    class Registry(asset.Registry):
        """Fixture registry implementation"""

        def projects(self) -> typing.Iterable[str]:
            return content.keys()

        def lineages(self, project: asset.Project.Key) -> typing.Iterable[asset.Lineage.Key]:
            return content[project].keys()

        def generations(
            self, project: asset.Project.Key, lineage: asset.Lineage.Key
        ) -> typing.Iterable[asset.Generation.Key]:
            try:
                return content[project][lineage][1].keys()
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid lineage ({lineage})') from err

        def pull(self, project: asset.Project.Key, lineage: asset.Lineage.Key) -> prj.Package:
            return content[project][lineage][0]

        def push(self, package: prj.Package) -> None:
            raise NotImplementedError()

        def read(
            self,
            project: asset.Project.Key,
            lineage: asset.Lineage.Key,
            generation: asset.Generation.Key,
            sid: uuid.UUID,
        ) -> bytes:
            if sid not in content[project][lineage][1][generation][0].states:
                raise asset.Level.Invalid(f'Invalid state id ({sid})')
            idx = content[project][lineage][1][generation][0].states.index(sid)
            return content[project][lineage][1][generation][1][idx]

        def write(self, project: asset.Project.Key, lineage: asset.Lineage.Key, sid: uuid.UUID, state: bytes) -> None:
            raise NotImplementedError()

        def open(
            self, project: asset.Project.Key, lineage: asset.Lineage.Key, generation: asset.Generation.Key
        ) -> asset.Tag:
            try:
                return content[project][lineage][1][generation][0]
            except KeyError as err:
                raise asset.Level.Invalid(f'Invalid generation ({lineage}.{generation})') from err

        def close(
            self,
            project: asset.Project.Key,
            lineage: asset.Lineage.Key,
            generation: asset.Generation.Key,
            tag: asset.Tag,
        ) -> None:
            raise NotImplementedError()

    return Registry()


@pytest.fixture(scope='function')
def directory(registry: asset.Registry) -> asset.Directory:
    """Directory root fixture."""
    return asset.Directory(registry)


@pytest.fixture(scope='function')
def valid_instance(
    project_name: asset.Project.Key,
    populated_lineage: asset.Lineage.Key,
    valid_generation: asset.Generation.Key,
    directory: asset.Directory,
) -> asset.Instance:
    """Lineage fixture."""
    return asset.Instance(project_name, populated_lineage, valid_generation, directory)
