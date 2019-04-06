import uuid

from forml import project as prjmod
from forml.runtime import persistent, resource


class Registry(persistent.Registry):
    def _lineages(self, project: str) -> 'persistent.Level.Listing':
        pass

    def _generations(self, project: str, lineage: int) -> 'persistent.Level.Listing':
        pass

    def _pull(self, project: str, lineage: int) -> prjmod.Artifact:
        pass

    def _push(self, project: str, lineage: int, artifact: prjmod.Artifact) -> None:
        pass

    def _read(self, project: str, lineage: int, sid: uuid.UUID) -> bytes:
        pass

    def _write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        pass

    def _open(self, project: str, lineage: int, generation: int) -> resource.Record:
        pass

    def _close(self, project: str, lineage: int, generation: int, record: resource.Record) -> None:
        pass