"""
Null registry is a dummy registry implementation that doesn't persist anything.
"""
import typing
import uuid

from forml import project as prjmod, project
from forml.runtime.asset import directory, persistent


class Registry(persistent.Registry):
    """Dummy registry implementation.
    """
    def __init__(self, artifact: project.Artifact):
        self._artifact: project.Artifact = artifact

    def lineages(self, project: str) -> directory.Level.Listing:
        return directory.Level.Listing([0])

    def generations(self, project: str, lineage: int) -> directory.Level.Listing:
        return directory.Level.Listing([0])

    def pull(self, project: str, lineage: int) -> prjmod.Artifact:
        return self._artifact

    def push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        return

    def read(self, project: str, lineage: int, generation: int, sid: uuid.UUID) -> bytes:
        return bytes()

    def write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        return

    def open(self, project: str, lineage: int, generation: int) -> directory.Generation.Tag:
        return directory.Generation.Tag()

    def close(self, project: str, lineage: int, generation: int, tag: directory.Generation.Tag) -> None:
        return
