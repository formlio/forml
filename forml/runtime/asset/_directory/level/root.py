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

from ... import _directory, _persistent
from . import case as prjmod

LOGGER = logging.getLogger(__name__)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Directory(_directory.Level):
    """Sequence of projects."""

    def __init__(self, registry: '_persistent.Registry'):  # pylint: disable=useless-super-delegation
        super().__init__()
        self._registry: _persistent.Registry = registry

    def __hash__(self):
        return hash(self.registry)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.registry == self.registry

    def __repr__(self):
        return repr(self._registry)

    @property
    def registry(self) -> '_persistent.Registry':
        """Registry instance.

        Returns:
            Registry instance.
        """
        return self._registry

    def list(self) -> _directory.Level.Listing:
        """List the content of this level.

        Returns:
            Level content listing.
        """
        return self.Listing(prjmod.Project.Key(k) for k in self.registry.projects())

    def get(self, key: typing.Union[str, prjmod.Project.Key]) -> 'prjmod.Project':
        """Get a project instance by its name.

        Args:
            key: Project name.

        Returns:
            Project instance.
        """
        return prjmod.Project(self, key)

    @property
    def key(self) -> None:
        """No key for the root."""
        return None
