"""Generic assets directory.
"""
import logging

from forml.runtime.asset import persistent, directory
from forml.runtime.asset.directory import project as prjmod

LOGGER = logging.getLogger(__name__)


# pylint: disable=unsubscriptable-object; https://github.com/PyCQA/pylint/issues/2822
class Level(directory.Level[None, str]):
    """Sequence of projects.
    """
    def __init__(self, registry: 'persistent.Registry'):  # pylint: disable=useless-super-delegation
        super().__init__()
        self._registry: persistent.Registry = registry

    def __hash__(self):
        return hash(self.registry)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.registry == self.registry

    def __str__(self):
        return str(self._registry)

    @property
    def registry(self) -> 'persistent.Registry':
        """Registry instance.

        Returns: Registry instance.
        """
        return self._registry

    def list(self) -> directory.Level.Listing[str]:
        """List the content of this level.

        Returns: Level content listing.
        """
        return self.Listing(self.registry.projects())

    def get(self, project: str) -> 'prjmod.Level':
        """Get a project instance by its name.

        Args:
            project: Project name.

        Returns: Project instance.
        """
        return prjmod.Level(self, project)

    @property
    def key(self) -> None:
        """No key for the root.
        """
        return None
