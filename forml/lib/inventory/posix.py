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
from forml.runtime import asset

LOGGER = logging.getLogger(__name__)


class Path(pathlib.Path):
    """Inventory path utility."""

    APPSFX = 'py'

    def descriptor(self, application: str) -> pathlib.Path:
        return self / f'{application}.{self.APPSFX}'

    def is_descriptor(self, path: pathlib.Path) -> bool:
        return path.is_file() and path.suffix == f'.{self.APPSFX}'


class Inventory(asset.Inventory, alias='posix'):
    """Posix inventory implementation."""

    def __init__(self, path: typing.Union[str, pathlib.Path] = conf.USRDIR / 'inventory'):
        self._path: Path = Path(pathlib.Path(path).resolve())

    def list(self) -> typing.Iterable[str]:
        return tuple(p.name for p in self._path.iterdir() if self._path.is_descriptor(p))

    def get(self, application: str) -> type[project.Descriptor]:
        path = self._path.descriptor(application)
        LOGGER.debug('Getting descriptor %s from %s', application, path)
        return project.Descriptor.Handle(path).descriptor

    def put(self, descriptor: project.Descriptor.Handle) -> None:
        path = self._path.descriptor(descriptor.application)
        LOGGER.debug('Putting descriptor %s to %s', descriptor.path, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(descriptor.path.read_text())
