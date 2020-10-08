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

from forml.project import distribution
from forml.runtime.asset import persistent, directory as dirmod, access
from forml.runtime.asset.directory import root as rootmod, project as prjmod, lineage as lngmod, generation as genmod


@pytest.fixture(scope='session')
def populated_lineage(project_lineage: lngmod.Level.Key) -> lngmod.Level.Key:
    """Lineage fixture.
    """
    return project_lineage


@pytest.fixture(scope='session')
def empty_lineage() -> lngmod.Level.Key:
    """Lineage fixture.
    """
    return lngmod.Level.Key('2')


@pytest.fixture(scope='session')
def last_lineage(empty_lineage: lngmod.Level.Key) -> lngmod.Level.Key:
    """Lineage fixture.
    """
    return empty_lineage


@pytest.fixture(scope='session')
def last_generation(valid_generation: genmod.Level.Key) -> genmod.Level.Key:
    """Generation fixture.
    """
    return valid_generation


@pytest.fixture(scope='function')
def registry(project_name: prjmod.Level.Key, populated_lineage: lngmod.Level.Key, empty_lineage: lngmod.Level.Key,
             valid_generation: genmod.Level.Key, tag: genmod.Tag,
             states: typing.Mapping[uuid.UUID, bytes], project_package: distribution.Package) -> persistent.Registry:
    """Registry fixture.
    """
    content = {project_name: {populated_lineage: (project_package,
                                                  {valid_generation: (tag, tuple(states.values()))}),
                              empty_lineage: (project_package, {})}
               }

    class Registry(persistent.Registry):
        """Fixture registry implementation
        """
        def projects(self) -> typing.Iterable[str]:
            return content.keys()

        def lineages(self, project: prjmod.Level.Key) -> typing.Iterable[lngmod.Level.Key]:
            return content[project].keys()

        def generations(self, project: prjmod.Level.Key,
                        lineage: lngmod.Level.Key) -> typing.Iterable[genmod.Level.Key]:
            try:
                return content[project][lineage][1].keys()
            except KeyError as err:
                raise dirmod.Level.Invalid(f'Invalid lineage ({lineage})') from err

        def pull(self, project: prjmod.Level.Key, lineage: lngmod.Level.Key) -> distribution.Package:
            return content[project][lineage][0]

        def push(self, package: distribution.Package) -> None:
            raise NotImplementedError()

        def read(self, project: prjmod.Level.Key, lineage: lngmod.Level.Key, generation: genmod.Level.Key,
                 sid: uuid.UUID) -> bytes:
            if sid not in content[project][lineage][1][generation][0].states:
                raise dirmod.Level.Invalid(f'Invalid state id ({sid})')
            idx = content[project][lineage][1][generation][0].states.index(sid)
            return content[project][lineage][1][generation][1][idx]

        def write(self, project: prjmod.Level.Key, lineage: lngmod.Level.Key, sid: uuid.UUID, state: bytes) -> None:
            raise NotImplementedError()

        def open(self, project: prjmod.Level.Key, lineage: lngmod.Level.Key,
                 generation: genmod.Level.Key) -> genmod.Tag:
            try:
                return content[project][lineage][1][generation][0]
            except KeyError as err:
                raise dirmod.Level.Invalid(f'Invalid generation ({lineage}.{generation})') from err

        def close(self, project: prjmod.Level.Key, lineage: lngmod.Level.Key, generation: genmod.Level.Key,
                  tag: genmod.Tag) -> None:
            raise NotImplementedError()

    return Registry()


@pytest.fixture(scope='function')
def directory(registry: persistent.Registry) -> rootmod.Level:
    """Directory root fixture.
    """
    return rootmod.Level(registry)


@pytest.fixture(scope='function')
def valid_assets(project_name: prjmod.Level.Key, populated_lineage: lngmod.Level.Key,
                 valid_generation: genmod.Level.Key, directory: rootmod.Level) -> access.Assets:
    """Lineage fixture.
    """
    return access.Assets(project_name, populated_lineage, valid_generation, directory)
