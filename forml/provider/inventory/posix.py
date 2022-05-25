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
Filesystem inventory is a plain hierarchical file based locally-accessible structure.
"""
import logging
import pathlib
import typing

from forml import conf, project
from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Path(type(pathlib.Path())):  # https://bugs.python.org/issue24132
    """Inventory path utility."""

    APPSFX = 'py'

    def descriptor(self, application: str) -> pathlib.Path:
        """Get the descriptor path for the given application."""
        return self / f'{application}.{self.APPSFX}'

    @classmethod
    def is_descriptor(cls, path: pathlib.Path) -> bool:
        """Check the give path is a descriptor path."""
        return path.is_file() and path.suffix == f'.{cls.APPSFX}'


class Inventory(asset.Inventory, alias='posix'):
    """Posix inventory implementation."""

    def __init__(self, path: typing.Union[str, pathlib.Path] = conf.USRDIR / 'inventory'):
        self._path: Path = Path(pathlib.Path(path).resolve())

    def list(self) -> typing.Iterable[str]:
        if not self._path.exists():
            return ()
        return tuple(p.stem for p in self._path.iterdir() if self._path.is_descriptor(p))

    def get(self, application: str) -> project.Descriptor:
        path = self._path.descriptor(application)
        LOGGER.debug('Getting descriptor %s from %s', application, path)
        return project.Descriptor.Handle(path).descriptor

    def put(self, descriptor: project.Descriptor.Handle) -> None:
        path = self._path.descriptor(descriptor.descriptor.name)
        LOGGER.debug('Putting descriptor %s to %s', descriptor.path, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(descriptor.path.read_text())
