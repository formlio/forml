"""
Virtual registry is a dummy registry implementation that doesn't persist anything outside of the current runtime.
"""
import logging
import os
import tempfile
import typing
import uuid

from forml import project as prjmod
from forml.project.component import virtual
from forml.runtime.asset import directory, persistent

LOGGER = logging.getLogger(__name__)


class Registry(persistent.Registry, key='virtual'):
    """Virtual registry implementation implemented as temporary directory deleted on exit.
    """
    _STATESFX = 'bin'
    _TAGFILE = 'tag.json'
    _TEMPS: typing.Dict[typing.Optional[str], tempfile.TemporaryDirectory] = dict()

    def __init__(self, path: typing.Optional[str] = None):
        self._artifacts: typing.Dict[str, prjmod.Artifact] = dict()
        if path not in self._TEMPS:
            self._TEMPS[path] = tempfile.TemporaryDirectory(prefix='forml-virtual-', dir=path)
        self._temp: tempfile.TemporaryDirectory = self._TEMPS[path]

    def _prjdir(self, project: str) -> str:
        """Get the project directory path.

        Args:
            project: Name of the project.

        Returns: Project directory path.
        """
        return os.path.join(self._temp.name, project)

    def _stfile(self, project: str, sid: uuid.UUID) -> str:
        """State file path of given sid an project name.

        Args:
            project: Name of the project.
            sid: State id.

        Returns: State file path.
        """
        return os.path.join(self._prjdir(project), f'{sid}.{self._STATESFX}')

    def lineages(self, project: str) -> directory.Level.Listing:
        return directory.Level.Listing([0])

    def generations(self, project: str, lineage: int) -> directory.Level.Listing:
        return directory.Level.Listing([0])

    def pull(self, project: str, lineage: int) -> 'prjmod.Artifact':
        os.makedirs(self._prjdir(project), exist_ok=True)
        if project not in self._artifacts:  # fallback to a virtual artifact
            package = f'{virtual.__name__}.{project}'
            self._artifacts[project] = prjmod.Artifact(
                source=f'{package}.source', pipeline=f'{package}.pipeline', evaluation=f'{package}.evaluation')
        return self._artifacts[project]

    def push(self, project: str, lineage: int, artifact: 'prjmod.Artifact') -> None:
        self._artifacts[project] = artifact

    def read(self, project: str, lineage: int, generation: int, sid: uuid.UUID) -> bytes:
        path = self._stfile(project, sid)
        LOGGER.debug('Reading state from %s', path)
        try:
            with open(path, 'rb') as storage:
                return storage.read()
        except FileNotFoundError:
            return bytes()

    def write(self, project: str, lineage: int, sid: uuid.UUID, state: bytes) -> None:
        path = self._stfile(project, sid)
        LOGGER.debug('Writing state of %d bytes to %s', len(state), path)
        with open(path, 'wb') as storage:
            storage.write(state)

    def open(self, project: str, lineage: int, generation: int) -> directory.Generation.Tag:
        try:
            with open(os.path.join(self._prjdir(project), self._TAGFILE), 'rb') as meta:
                return directory.Generation.Tag.loads(meta.read())
        except FileNotFoundError:
            raise directory.Level.Listing.Empty(f'Empty generation {generation} under {project}/{lineage}')

    def close(self, project: str, lineage: int, generation: int, tag: directory.Generation.Tag) -> None:
        with open(os.path.join(self._prjdir(project), self._TAGFILE), 'wb') as meta:
            meta.write(tag.dumps())
