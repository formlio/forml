"""Generic assets directory.
"""
import logging
import typing
import uuid

from packaging import version

from forml.runtime.asset import directory, persistent
from forml.runtime.asset.directory import generation as genmod

if typing.TYPE_CHECKING:
    from forml.project import product
    from forml.runtime.asset.directory import project as prjmod

LOGGER = logging.getLogger(__name__)
ARTIFACTS = directory.Cache(persistent.Registry.mount)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Level(directory.Level[version.Version, int]):
    """Sequence of generations based on same project artifact.
    """
    def __init__(self, project: 'prjmod.Level',
                 key: typing.Optional[typing.Union[str, version.Version]] = None):
        if key:
            key = version.Version(str(key))
        super().__init__(key, parent=project)

    @property
    def project(self) -> 'prjmod.Level':
        """Get the project of this generation.

        Returns: Project of this generation.
        """
        return self._parent

    @property
    def artifact(self) -> 'product.Artifact':
        """Lineage artifact.

        Returns: Artifact object.
        """
        return ARTIFACTS(self.registry, self.project.key, self.key)

    def dump(self, state: bytes) -> uuid.UUID:
        """Dump an unbound state (not belonging to any project) under given state id.

        An unbound state is expected to be committed later into a new generation of specific lineage.

        Args:
            state: Serialized state to be persisted.

        Returns: Associated state id.
        """
        sid = uuid.uuid4()
        LOGGER.debug('%s: Dumping state %s', self, sid)
        self.registry.write(self.project.key, self.key, sid, state)
        return sid

    def list(self) -> directory.Level.Listing[int]:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self.registry.generations(self.project.key, self.key)

    def get(self, generation: typing.Optional[int] = None) -> genmod.Level:
        """Get a generation instance by its id.

        Args:
            generation: Integer generation id.

        Returns: genmod.Level instance.
        """
        return genmod.Level(self, generation)

    def put(self, tag: genmod.Tag) -> genmod.Level:
        """Commit a new generation described by its tag. All states listed on the tag are expected to have
        been provided previously by individual dumps.

        Args:
            tag: genmod.Level metadata.

        Returns: genmod.Level instance.
        """
        try:
            generation = self.list().last + 1
        except self.Listing.Empty:
            generation = 1
        self.registry.close(self.project.key, self.key, generation, tag)
        return self.get(generation)
