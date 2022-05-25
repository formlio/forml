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

import forml

from ... import _directory
from . import major as lngmod

if typing.TYPE_CHECKING:
    from forml import project as prj

    from . import root as rootmod

LOGGER = logging.getLogger(__name__)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Project(_directory.Level):
    """Sequence of releases based on same project."""

    class Key(_directory.Level.Key, str):  # pylint: disable=abstract-method
        """Project level key."""

    def __init__(self, root: 'rootmod.Directory', key: typing.Union[str, 'Project.Key']):
        super().__init__(key, parent=root)

    def list(self) -> _directory.Level.Listing:
        """List the content of this level.

        Returns:
            Level content listing.
        """
        return self.Listing(lngmod.Release.Key(n) for n in self.registry.releases(self.key))

    def get(self, key: typing.Optional[typing.Union[str, lngmod.Release.Key]] = None) -> lngmod.Release:
        """Get a release instance by its id.

        Args:
            key: Release version.

        Returns:
            Release instance.
        """
        return lngmod.Release(self, key)

    def put(self, package: 'prj.Package') -> lngmod.Release:
        """Publish new release to the repository based on provided package.

        Args:
            package: Distribution package to be persisted.

        Returns:
            new release instance based on the package.
        """
        project = package.manifest.name
        release = package.manifest.version
        try:
            previous = self.list().last
        except (_directory.Level.Invalid, _directory.Level.Listing.Empty):
            LOGGER.debug('No previous release for %s-%s', project, release)
        else:
            if project != self.key:
                raise forml.InvalidError('Project key mismatch')
            if not release > previous:
                raise _directory.Level.Invalid(f'{project}-{release} not an increment from existing {previous}')
        self.registry.push(package)
        return self.get(release)
