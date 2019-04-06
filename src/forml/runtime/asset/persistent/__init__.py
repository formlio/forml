"""
ForML assets persistence.
"""

import abc
import typing
import uuid

from forml import project as prjmod
from forml.runtime.asset import directory


class Registry(metaclass=abc.ABCMeta):
    """Top-level persistent registry abstraction.
    """
    def __str__(self):
        return f'{self.__class__.__name__}-registry'

    def get(self, project: str, lineage: typing.Optional[int] = None) -> 'directory.Lineage':
        """Get a lineage of given project.

        Args:
            project: Name of the project to work with.
            lineage: Optional lineage id to be loaded (defaults to the most recent id).

        Returns: Lineage instance.
        """
        return directory.Lineage(self, project, lineage)

    def put(self, project: str, artifact: prjmod.Artifact) -> 'directory.Lineage':
        """Publish new lineage to the repository based on provided artifact.

        Args:
            project: Project the lineage belongs to.
            artifact: Artifact to be published.

        Returns: new lineage instance based on the artifact.
        """
        lineage = self.lineages(project).next
        self.push(project, lineage, artifact)
        return self.get(project, lineage)

    @abc.abstractmethod
    def lineages(self, project: str) -> 'directory.Level.Listing':
        """List the lineages of given project.

        Args:
            project: Project to be listed.

        Returns: Lineages listing.
        """

    @abc.abstractmethod
    def generations(self, project: str, lineage: int) -> 'directory.Level.Listing':
        """List the generations of given lineage.

        Args:
            project: Project of which the lineage is to be listed.
            lineage: Lineage of the project to be listed.

        Returns: Generations listing.
        """

    @abc.abstractmethod
    def pull(self, project: str, lineage: int) -> prjmod.Artifact:
        """Return the artifact of given lineage.

        Args:
            project: Project of which the lineage artifact is to be returned.
            lineage: Lineage of the project to return the artifact of.

        Returns: Project artifact object.
        """

    @abc.abstractmethod
    def push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        """Start new lineage of a project based on given artifact.

        Args:
            project: Project to start the new lineage in.
            artifact: Artifact of the new lineage.
        """

    @abc.abstractmethod
    def read(self, project: str, lineage: int, generation: int, sid: uuid.UUID) -> bytes:
        """Load the state based on provided id.

        Args:
            project: Project to read the state from.
            lineage: Lineage of the project to read the state from.
            generation: Generation of the project to read the state from.
            sid: Id of the state object to be loaded.

        Returns: Serialized state.
        """

    @abc.abstractmethod
    def write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        """Dump an unbound state under given state id.

        Args:
            project: Project to store the state into.
            lineage: Lineage of the project to store the state into.
            sid: state id to associate the payload with.
            state: Serialized state to be persisted.
        """

    @abc.abstractmethod
    def open(self, project: str, lineage: int, generation: int) -> 'directory.Generation.Tag':
        """Return the metadata tag of given generation.

        Args:
            project: Project to read the metadata from.
            lineage: Lineage of the project to read the metadata from.
            generation: Generation of the project to read the metadata from.

        Returns: Generation metadata.
        """

    @abc.abstractmethod
    def close(self, project: str, lineage: int, generation: int, tag: 'directory.Generation.Tag') -> None:
        """Seal new generation by storing its metadata tag.

        Args:
            project: Project to store the metadata into.
            lineage: directory.Lineage of the project to store the metadata into.
            generation: Generation of the project to store the metadata into.
            tag: Generation metadata to be stored.
        """
