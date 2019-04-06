"""
Local filesystem based implementation of persisten registry.
"""
import uuid

from forml import project as prjmod
from forml.runtime.asset import directory, persistent


class Registry(persistent.Registry):
    """Local registry.
    """
    def lineages(self, project: str) -> directory.Level.Listing:
        pass

    def generations(self, project: str, lineage: int) -> directory.Level.Listing:
        pass

    def pull(self, project: str, lineage: int) -> prjmod.Artifact:
        pass

    def push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        pass

    def read(self, project: str, lineage: int, generation: int, sid: uuid.UUID) -> bytes:
        pass

    def write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        pass

    def open(self, project: str, lineage: int, generation: int) -> directory.Generation.Tag:
        pass

    def close(self, project: str, lineage: int, generation: int, tag: directory.Generation.Tag) -> None:
        pass
