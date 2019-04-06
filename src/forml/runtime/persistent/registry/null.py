"""
Null registry is a dummy registry implementation that doesn't persist anything.
"""
import uuid

from forml import project as prjmod
from forml.runtime import persistent, resource


class Registry(persistent.Registry):
    """Dummy registry implementation.
    """
    def _lineages(self, project: str) -> 'persistent.Level.Listing':
        return persistent.Level.Listing([])

    def _generations(self, project: str, lineage: int) -> 'persistent.Level.Listing':
        return persistent.Level.Listing([])

    def _pull(self, project: str, lineage: int) -> prjmod.Artifact:
        pass

    def _push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        return

    def _read(self, project: str, lineage: int, sid: uuid.UUID) -> bytes:
        return bytes()

    def _write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        return

    def _close(self, project: str, lineage: int, generation: int, record: resource.Record) -> None:
        return
