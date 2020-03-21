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
from forml.project import distribution
from forml.runtime.asset import directory, persistent
from forml.runtime.asset.directory import lineage as lngmod, generation as genmod

LOGGER = logging.getLogger(__name__)


class Path(type(pathlib.Path())):  # https://bugs.python.org/issue24132
    """Repository path utility.
    """
    class Matcher(metaclass=abc.ABCMeta):
        """Registry component validator.
        """

        @staticmethod
        @abc.abstractmethod
        def constructor(key: str) -> 'directory.KeyT':
            """Level key constructor.

            Args:
                key: Key text value.

            Returns: Key instance.
            """

        @classmethod
        def key(cls, path: pathlib.Path) -> bool:
            """Check given path is a valid registry level key.

            Args:
                path: Path to be checked.

            Returns: True if valid.
            """

            def constructs(name: str) -> bool:
                try:
                    cls.constructor(name)
                except (TypeError, ValueError, lngmod.Version.Invalid):
                    return False
                return True

            return path.is_dir() and constructs(path.name)

        @staticmethod
        @abc.abstractmethod
        def content(level: pathlib.Path) -> bool:
            """Test the level content is valid for given level.

            Args:
                level: Root path of given level

            Returns: True if valid.
            """

        @classmethod
        def valid(cls, path: pathlib.Path) -> bool:
            """Check given path is a valid level.

            Args:
                path: Path to be checked.

            Returns: True if valid.
            """
            if not cls.key(path):
                LOGGER.debug('Path %s is not a valid level key', path)
                return False
            if not cls.content(path):
                LOGGER.debug('Path %s does not have a valid level content', path)
                return False
            return True

    class Project(Matcher):
        """Project matcher.
        """
        constructor = staticmethod(str)

        @staticmethod
        def content(root: pathlib.Path) -> bool:
            return any(Path.Lineage.valid(l) for l in root.iterdir())

    class Lineage(Matcher):
        """Lineage matcher.
        """
        constructor = staticmethod(lngmod.Version)

        @staticmethod
        def content(root: pathlib.Path) -> bool:
            return (root / Path.PKGFILE).exists()

    class Generation(Matcher):
        """Generation matcher.
        """
        constructor = staticmethod(int)

        @staticmethod
        def content(root: pathlib.Path) -> bool:
            return (root / Path.TAGFILE).exists()

    STAGEDIR = '.stage'
    STATESFX = 'bin'
    TAGFILE = 'tag.json'
    PKGFILE = f'package.{distribution.Package.FORMAT}'

    @functools.lru_cache()
    def project(self, project: str) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.

        Returns: Project directory path.
        """
        return self / project

    @functools.lru_cache()
    def lineage(self, project: str, lineage: lngmod.Version) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.
            lineage: Lineage key.

        Returns: Project directory path.
        """
        return self.project(project) / str(lineage)

    @functools.lru_cache()
    def generation(self, project: str, lineage: lngmod.Version, generation: int) -> pathlib.Path:
        """Get the project directory path.

        Args:
            project: Name of the project.
            lineage: Lineage key.
            generation: Generation key.

        Returns: Project directory path.
        """
        return self.lineage(project, lineage) / str(generation)

    @functools.lru_cache()
    def package(self, project: str, lineage: lngmod.Version) -> pathlib.Path:
        """Package file path of given project name/lineage.

        Args:
            project: Name of the project.
            lineage: Lineage key.

        Returns: Package file path.
        """
        return self.lineage(project, lineage) / self.PKGFILE

    @functools.lru_cache()
    def state(self, sid: uuid.UUID, project: str, lineage: lngmod.Version,
              generation: typing.Optional[int] = None) -> pathlib.Path:
        """State file path of given sid an project name.

        Args:
            project: Name of the project.
            lineage: Lineage key.
            generation: Generation key.
            sid: State id.

        Returns: State file path.
        """
        if generation is None:
            generation = self.STAGEDIR
        return self.generation(project, lineage, generation) / f'{sid}.{self.STATESFX}'

    @functools.lru_cache()
    def tag(self, project: str, lineage: lngmod.Version, generation: int) -> pathlib.Path:
        """Tag file path of given project name.

        Args:
            project: Name of the project.
            lineage: Lineage key.
            generation: Generation key.

        Returns: Tag file path.
        """
        return self.generation(project, lineage, generation) / self.TAGFILE


class Registry(persistent.Registry, key='filesystem'):
    """Filesystem registry is a locally-accessible file based hierarchy.
    """
    def __init__(self, path: typing.Union[str, pathlib.Path] = conf.USRDIR / 'registry',
                 staging: typing.Optional[typing.Union[str, pathlib.Path]] = None):
        path = pathlib.Path(path).resolve()
        super().__init__(staging or path / Path.STAGEDIR)
        self._path: Path = Path(path)

    @staticmethod
    def _listing(path: pathlib.Path, matcher: typing.Type[Path.Matcher]) -> typing.Iterable:
        """Helper for listing given repository level.

        Args:
            path: Path to be listed.
            matcher: Item matcher.

        Returns: Repository level listing.
        """
        try:
            return [matcher.constructor(p.name) for p in path.iterdir() if matcher.valid(p)]
        except NotADirectoryError:
            raise directory.Level.Invalid(f'Path {path} is not a valid registry component')
        except FileNotFoundError:
            return tuple()

    def projects(self) -> typing.Iterable[str]:
        return self._listing(self._path, Path.Project)

    def lineages(self, project: str) -> typing.Iterable[str]:
        return self._listing(self._path.project(project), Path.Lineage)

    def generations(self, project: str, lineage: lngmod.Version) -> typing.Iterable[int]:
        return self._listing(self._path.lineage(project, lineage), Path.Generation)

    def pull(self, project: str, lineage: lngmod.Version) -> 'distribution.Package':
        return distribution.Package(self._path.package(project, lineage))

    def push(self, package: 'distribution.Package') -> None:
        project = package.manifest.name
        lineage = package.manifest.version
        path = self._path.package(project, lineage)
        path.parent.mkdir(parents=True, exist_ok=True)
        if package.path.is_dir():
            shutil.copytree(package.path, path, ignore=lambda *_: {'__pycache__'})
        else:
            assert package.path.is_file(), 'Expecting file package'
            path.write_bytes(package.path.read_bytes())

    def read(self, project: str, lineage: lngmod.Version, generation: int, sid: uuid.UUID) -> bytes:
        path = self._path.state(sid, project, lineage, generation)
        LOGGER.debug('Reading state from %s', path)
        if not path.parent.exists():
            raise directory.Level.Invalid(f'Invalid registry component {project}/{lineage}/{generation}')
        try:
            with path.open('rb') as statefile:
                return statefile.read()
        except FileNotFoundError:
            LOGGER.warning('No state %s under %s', sid, path)
            return bytes()

    def write(self, project: str, lineage: lngmod.Version, sid: uuid.UUID, state: bytes) -> None:
        path = self._path.state(sid, project, lineage)
        LOGGER.debug('Staging state of %d bytes to %s', len(state), path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('wb') as statefile:
            statefile.write(state)

    def open(self, project: str, lineage: lngmod.Version, generation: int) -> 'genmod.Tag':
        path = self._path.tag(project, lineage, generation)
        try:
            with path.open('rb') as tagfile:
                return genmod.Tag.loads(tagfile.read())
        except FileNotFoundError:
            raise directory.Level.Listing.Empty(f'No tag under {path}')

    def close(self, project: str, lineage: lngmod.Version, generation: int, tag: 'genmod.Tag') -> None:
        path = self._path.tag(project, lineage, generation)
        LOGGER.debug('Committing states of tag %s as %s', tag, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        for sid in tag.states:
            source = self._path.state(sid, project, lineage)
            if not source.exists():
                raise directory.Level.Invalid(f'State {sid} not staged')
            target = self._path.state(sid, project, lineage, generation)
            source.rename(target)
        with path.open('wb') as tagfile:
            tagfile.write(tag.dumps())
