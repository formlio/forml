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

"""Generic assets directory.
"""
import logging
import typing

from forml import error  # pylint: disable=unused-import; # noqa: F401
from forml.runtime.asset import directory
from forml.runtime.asset.directory import lineage as lngmod

if typing.TYPE_CHECKING:
    from forml.project import distribution
    from forml.runtime.asset.directory import root as rootmod

LOGGER = logging.getLogger(__name__)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Level(directory.Level):
    """Sequence of lineages based on same project."""

    class Key(directory.Level.Key, str):  # pylint: disable=abstract-method
        """Project level key."""

    def __init__(self, root: 'rootmod.Level', key: typing.Union[str, 'Level.Key']):
        super().__init__(key, parent=root)

    def list(self) -> directory.Level.Listing:
        """List the content of this level.

        Returns:
            Level content listing.
        """
        return self.Listing(lngmod.Level.Key(n) for n in self.registry.lineages(self.key))

    def get(self, lineage: typing.Optional[typing.Union[str, lngmod.Level.Key]] = None) -> lngmod.Level:
        """Get a lineage instance by its id.

        Args:
            lineage: Lineage version.

        Returns:
            Lineage instance.
        """
        return lngmod.Level(self, lineage)

    def put(self, package: 'distribution.Package') -> lngmod.Level:
        """Publish new lineage to the repository based on provided package.

        Args:
            package: Distribution package to be persisted.

        Returns:
            new lineage instance based on the package.
        """
        project = package.manifest.name
        lineage = package.manifest.version
        try:
            previous = self.list().last
        except (directory.Level.Invalid, directory.Level.Listing.Empty):
            LOGGER.debug('No previous lineage for %s-%s', project, lineage)
        else:
            if project != self.key:
                raise error.Invalid('Project key mismatch')
            if not lineage > previous:
                raise directory.Level.Invalid(f'{project}-{lineage} not an increment from existing {previous}')
        self.registry.push(package)
        return self.get(lineage)
