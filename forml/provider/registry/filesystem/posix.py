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
Filesystem registry is a plain hierarchical file based locally-accessible structure.
"""
import abc
import functools
import logging
import pathlib
import shutil
import typing
import uuid

from forml import conf
from forml import project as prj
from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Path(type(pathlib.Path())):  # https://bugs.python.org/issue24132
    """Repository path utility."""

    class Matcher(metaclass=abc.ABCMeta):
        """Registry component validator."""

        @staticmethod
        @abc.abstractmethod
        def constructor(key: str) -> asset.Level.Key:
            """Level key constructor.

            Args:
                key: Key text value.

            Returns:
                Key instance.
            """

        @classmethod
        def key(cls, path: pathlib.Path) -> bool:
            """Check given path is a valid registry level key.

            Args:
                path: Path to be checked.

            Returns:
                True if valid.
            """

            def constructs(name: str) -> bool:
                try:
                    cls.constructor(name)
                except (TypeError, ValueError):
                    return False
                return True

            return path.is_dir() and constructs(path.name)

        @staticmethod
        @abc.abstractmethod
        def content(level: pathlib.Path) -> bool:
            """Test the level content is valid for given level.

            Args:
                level: Root path of given level

            Returns:
                True if valid.
            """

        @classmethod
        def valid(cls, path: pathlib.Path) -> bool:
            """Check given path is a valid level.

            Args:
                path: Path to be checked.

            Returns:
                True if valid.
            """
            if not cls.key(path):
                LOGGER.debug('Path %s is not a valid level key', path)
                return False
            if not cls.content(path):
                LOGGER.debug('Path %s does not have a valid level content', path)
                return False
            return True

    class Project(Matcher):
        """Project matcher."""

        constructor = staticmethod(asset.Project.Key)

        @staticmethod
        def content(level: pathlib.Path) -> bool:
            return any(Path.Release.valid(i) for i in level.iterdir())

    class Release(Matcher):
        """Release matcher."""

        constructor = staticmethod(asset.Release.Key)

        @staticmethod
        def content(level: pathlib.Path) -> bool:
            return (level / Path.PKGFILE).exists()

    class Generation(Matcher):
        """Generation matcher."""

        constructor = staticmethod(asset.Generation.Key)

        @staticmethod
        def content(level: pathlib.Path) -> bool:
            return (level / Path.TAGFILE).exists()

    STAGEDIR = '.stage'
    STATESFX = 'bin'
    TAGFILE = 'tag.toml'
    PKGFILE = f'package.{prj.Package.FORMAT}'

    @functools.cache
    def project(self, project: asset.Project.Key) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.

        Returns:
            Project directory path.
        """
        return self / project

    @functools.cache
    def release(self, project: asset.Project.Key, release: asset.Release.Key) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.
            release: Release key.

        Returns:
            Project directory path.
        """
        return self.project(project) / str(release)

    @functools.cache
    def generation(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
    ) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.
            release: Release key.
            generation: Generation key.

        Returns:
            Project directory path.
        """
        return self.release(project, release) / str(generation)

    @functools.cache
    def package(self, project: asset.Project.Key, release: asset.Release.Key) -> pathlib.Path:
        """Package file path of given project name/release.

        Args:
            project: Name of the project.
            release: Release key.

        Returns:
            Package file path.
        """
        return self.release(project, release) / self.PKGFILE

    @functools.cache
    def state(
        self,
        sid: uuid.UUID,
        project: asset.Project.Key,
        release: asset.Release.Key,
        generation: typing.Optional[asset.Generation.Key] = None,
    ) -> pathlib.Path:
        """State file path of given sid an project name.

        Args:
            project: Name of the project.
            release: Release key.
            generation: Generation key.
            sid: State id.

        Returns:
            State file path.
        """
        if generation is None:
            generation = self.STAGEDIR
        return self.generation(project, release, generation) / f'{sid}.{self.STATESFX}'

    @functools.cache
    def tag(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
    ) -> pathlib.Path:
        """Tag file path of given project name.

        Args:
            project: Name of the project.
            release: Release key.
            generation: Generation key.

        Returns:
            Tag file path.
        """
        return self.generation(project, release, generation) / self.TAGFILE


class Registry(asset.Registry, alias='posix'):
    """Filesystem registry is a locally-accessible file based hierarchy."""

    def __init__(
        self,
        path: typing.Union[str, pathlib.Path] = conf.USRDIR / 'registry',
        staging: typing.Optional[typing.Union[str, pathlib.Path]] = None,
    ):
        path = pathlib.Path(path).resolve()
        super().__init__(staging or path / Path.STAGEDIR)
        self._path: Path = Path(path)

    @staticmethod
    def _listing(path: pathlib.Path, matcher: type[Path.Matcher]) -> typing.Iterable:
        """Helper for listing given repository level.

        Args:
            path: Path to be listed.
            matcher: Item matcher.

        Returns:
            Repository level listing.
        """
        try:
            return [matcher.constructor(p.name) for p in path.iterdir() if matcher.valid(p)]
        except NotADirectoryError as err:
            raise asset.Level.Invalid(f'Path {path} is not a valid registry component') from err
        except FileNotFoundError:
            return ()

    def projects(self) -> typing.Iterable[asset.Project.Key]:
        return self._listing(self._path, Path.Project)

    def releases(self, project: asset.Project.Key) -> typing.Iterable[asset.Release.Key]:
        return self._listing(self._path.project(project), Path.Release)

    def generations(
        self, project: asset.Project.Key, release: asset.Release.Key
    ) -> typing.Iterable[asset.Generation.Key]:
        return self._listing(self._path.release(project, release), Path.Generation)

    def pull(self, project: asset.Project.Key, release: asset.Release.Key) -> 'prj.Package':
        return prj.Package(self._path.package(project, release))

    def push(self, package: 'prj.Package') -> None:
        project = package.manifest.name
        release = package.manifest.version
        path = self._path.package(project, release)
        path.parent.mkdir(parents=True, exist_ok=True)
        if package.path.is_dir():
            shutil.copytree(package.path, path, ignore=lambda *_: {'__pycache__'})
        else:
            assert package.path.is_file(), 'Expecting file package'
            path.write_bytes(package.path.read_bytes())

    def read(
        self,
        project: asset.Project.Key,
        release: asset.Release.Key,
        generation: asset.Generation.Key,
        sid: uuid.UUID,
    ) -> bytes:
        path = self._path.state(sid, project, release, generation)
        LOGGER.debug('Reading state from %s', path)
        if not path.parent.exists():
            raise asset.Level.Invalid(f'Invalid registry component {project}/{release}/{generation}')
        try:
            with path.open('rb') as statefile:
                return statefile.read()
        except FileNotFoundError:
            LOGGER.warning('No state %s under %s', sid, path)
            return bytes()

    def write(self, project: asset.Project.Key, release: asset.Release.Key, sid: uuid.UUID, state: bytes) -> None:
        path = self._path.state(sid, project, release)
        LOGGER.debug('Staging state of %d bytes to %s', len(state), path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as statefile:
            statefile.write(state)

    def open(
        self, project: asset.Project.Key, release: asset.Release.Key, generation: asset.Generation.Key
    ) -> asset.Tag:
        path = self._path.tag(project, release, generation)
        try:
            with path.open('rb') as tagfile:
                return asset.Tag.loads(tagfile.read())
        except FileNotFoundError as err:
            raise asset.Level.Listing.Empty(f'No tag under {path}') from err

    def close(
        self,
        project: asset.Project.Key,
        release: asset.Release.Key,
        generation: asset.Generation.Key,
        tag: asset.Tag,
    ) -> None:
        path = self._path.tag(project, release, generation)
        LOGGER.debug('Committing states of tag %s as %s', tag, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        for sid in tag.states:
            source = self._path.state(sid, project, release)
            if not source.exists():
                raise asset.Level.Invalid(f'State {sid} not staged')
            target = self._path.state(sid, project, release, generation)
            source.rename(target)
        with path.open('wb') as tagfile:
            tagfile.write(tag.dumps())
