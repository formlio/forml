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
Virtual registry is a dummy registry implementation that doesn't persist anything outside of the current runtime.
"""
import collections
import logging
import tempfile
import typing

from forml.runtime.asset import persistent
from forml.lib.registry import filesystem

if typing.TYPE_CHECKING:
    from forml.project import product, distribution  # pylint: disable=unused-import
    from forml.runtime.asset.directory import project as prjmod, lineage as lngmod

LOGGER = logging.getLogger(__name__)


class Registry(filesystem.Registry, alias='virtual'):
    """Virtual registry implementation provided as a non-distributed global registry persistent only during its
    lifetime.
    """

    def __init__(self):
        self._storage: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory(
            prefix='registry-virtual-', dir=persistent.TMPDIR.name
        )
        self._artifacts: typing.Dict[
            'prjmod.Level.Key', typing.Dict['lngmod.Level.Key', 'product.Artifact']
        ] = collections.defaultdict(dict)
        super().__init__(self._storage.name)

    def projects(self) -> typing.Iterable['prjmod.Level.Key']:
        return iter(self._artifacts.keys())

    def lineages(self, project: 'prjmod.Level.Key') -> typing.Iterable['lngmod.Level.Key']:
        return iter(self._artifacts[project].keys())

    def mount(self, project: 'prjmod.Level.Key', lineage: 'lngmod.Level.Key') -> 'product.Artifact':
        return self._artifacts[project][lineage]

    def pull(self, project: 'prjmod.Level.Key', lineage: 'lngmod.Level.Key') -> 'distribution.Package':
        raise NotImplementedError('No packages in virtual repository')

    def push(self, package: 'distribution.Package') -> None:
        artifact = package.install(package.path)  # avoid copying by installing to self
        self._artifacts[package.manifest.name][package.manifest.version] = artifact
