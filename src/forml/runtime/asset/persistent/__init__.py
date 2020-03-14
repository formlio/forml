"""
ForML assets persistence.
"""
import abc
import functools
import logging
import pathlib
import tempfile
import typing
import uuid

from packaging import version

from forml import provider, conf
from forml.runtime.asset import directory

if typing.TYPE_CHECKING:
    from forml.project import distribution, product

LOGGER = logging.getLogger(__name__)
TMPDIR = tempfile.TemporaryDirectory(prefix=f'{conf.APPNAME}-persistent-', dir=conf.TMPDIR)


def mkdtemp(prefix: typing.Optional[str] = None, suffix: typing.Optional[str] = None) -> pathlib.Path:
    """Custom temp-dir maker that puts all temps under our global temp.

    Args:
        prefix: Optional temp dir prefix.
        suffix: Optional temp dir suffix.

    Returns: Temp dir as pathlib path.
    """
    return pathlib.Path(tempfile.mkdtemp(prefix, suffix, TMPDIR.name))


class Existing:
    """Decorators for verifying existence of given registry levels.
    """
    @staticmethod
    def project(method: typing.Callable) -> typing.Callable:
        """Decorator for registry methods that require existing project.

        Args:
            method: Registry method to be decorated.

        Returns: Decorated method with enforced project existence.
        """
        @functools.wraps(method)
        def wrapped(registry: 'Registry', project: str, *args, **kwargs) -> typing.Any:
            """Wrapped registry method.
            """
            if project not in registry.projects():
                raise directory.Level.Invalid(f'Unknown project {project}')
            return method(registry, project, *args, **kwargs)
        return wrapped

    @staticmethod
    def lineage(method: typing.Callable) -> typing.Callable:
        """Decorator for registry methods that require existing lineage.

        Args:
            method: Registry method to be decorated.

        Returns: Decorated method with enforced lineage existence.
        """
        @functools.wraps(method)
        def wrapped(registry: 'Registry', project: str, lineage: version.Version, *args, **kwargs) -> typing.Any:
            """Wrapped registry method.
            """
            if lineage not in registry.lineages(project):
                raise directory.Level.Invalid(f'Unknown lineage {lineage} for project {project}')
            return method(registry, project, lineage, *args, **kwargs)
        return wrapped

    @staticmethod
    def generation(method: typing.Callable) -> typing.Callable:
        """Decorator for registry methods that require existing generation.

        Args:
            method: Registry method to be decorated.

        Returns: Decorated method with enforced generation existence.
        """
        @functools.wraps(method)
        def wrapped(registry: 'Registry', project: str, lineage: version.Version,
                    generation: int, *args, **kwargs) -> typing.Any:
            """Wrapped registry method.
            """
            if generation not in registry.generations(project, lineage):
                raise directory.Level.Invalid(
                    f'Unknown generation {generation} for project {project}, lineage {lineage}')
            return method(registry, project, lineage, generation, *args, **kwargs)
        return wrapped


class Registry(provider.Interface, default=conf.REGISTRY):
    """Top-level persistent registry abstraction.
    """
    def __init__(self, staging: typing.Optional[typing.Union[str, pathlib.Path]] = None):
        if not staging:
            LOGGER.warning('Using temporal non-distributed staging for %s', self)
            staging = mkdtemp(prefix=f'{self}-staging-')
        self._staging: pathlib.Path = pathlib.Path(staging)

    def __str__(self):
        name = self.__class__.__module__.rsplit('.', 1)[-1].capitalize()
        return f'{name}-registry'

    def get(self, project: 'str') -> 'directory.Project':
        """Get the project handle.
        """
        return directory.Root(self).get(project)

    def mount(self, project: str, lineage: version.Version) -> 'product.Artifact':
        """Take given project/lineage package and return it as artifact instance.

        Args:
            project: Name of the project to work with.
            lineage: Lineage to be loaded.

        Returns: Product artifact.
        """
        package = self.pull(project, lineage)
        return package.install(self._staging / package.manifest.name / str(package.manifest.version))

    @abc.abstractmethod
    def projects(self) -> 'directory.Level.Listing[str]':
        """List projects in given repository.

        Returns: Projects listing.
        """

    @abc.abstractmethod
    def lineages(self, project: str) -> 'directory.Level.Listing[version.Version]':
        """List the lineages of given project.

        Args:
            project: Project to be listed.

        Returns: Lineages listing.
        """

    @abc.abstractmethod
    def generations(self, project: str, lineage: version.Version) -> 'directory.Level.Listing[int]':
        """List the generations of given lineage.

        Args:
            project: Project of which the lineage is to be listed.
            lineage: Lineage of the project to be listed.

        Returns: Generations listing.
        """

    @abc.abstractmethod
    def pull(self, project: str, lineage: version.Version) -> 'distribution.Package':
        """Return the package of given lineage.

        Args:
            project: Project of which the lineage artifact is to be returned.
            lineage: Lineage of the project to return the artifact of.

        Returns: Project artifact object.
        """

    @abc.abstractmethod
    def push(self, package: 'distribution.Package') -> None:
        """Start new lineage of a project based on given artifact.

        Args:
            package: Distribution package to be persisted.
        """

    @abc.abstractmethod
    def read(self, project: str, lineage: version.Version, generation: int, sid: uuid.UUID) -> bytes:
        """Load the state based on provided id.

        Args:
            project: Project to read the state from.
            lineage: Lineage of the project to read the state from.
            generation: Generation of the project to read the state from.
            sid: Id of the state object to be loaded.

        Returns: Serialized state or empty byte-array if there is no such state for given (existing) generation.
        """

    @abc.abstractmethod
    def write(self, project: str, lineage: version.Version, sid: uuid.UUID, state: bytes) -> None:
        """Dump an unbound state under given state id.

        Args:
            project: Project to store the state into.
            lineage: Lineage of the project to store the state into.
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """

    @abc.abstractmethod
    def open(self, project: str, lineage: version.Version, generation: int) -> 'directory.Generation.Tag':
        """Return the metadata tag of given generation.

        Args:
            project: Project to read the metadata from.
            lineage: Lineage of the project to read the metadata from.
            generation: Generation of the project to read the metadata from.

        Returns: Generation metadata.
        """

    @abc.abstractmethod
    def close(self, project: str, lineage: version.Version, generation: int, tag: 'directory.Generation.Tag') -> None:
        """Seal new generation by storing its metadata tag.

        Args:
            project: Project to store the metadata into.
            lineage: directory.Lineage of the project to store the metadata into.
            generation: Generation of the project to store the metadata into.
            tag: Generation metadata to be stored.
        """
