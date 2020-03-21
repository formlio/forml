"""
Virtual registry is a dummy registry implementation that doesn't persist anything outside of the current runtime.
"""
import collections
import logging
import tempfile
import typing

from forml.runtime.asset import persistent
from forml.runtime.asset.persistent.registry import filesystem

if typing.TYPE_CHECKING:
    from forml.project import product, distribution  # pylint: disable=unused-import
    from forml.runtime.asset.directory import lineage as lngmod

LOGGER = logging.getLogger(__name__)


class Registry(filesystem.Registry, key='virtual'):
    """Virtual registry implementation provided as a non-distributed global registry persistent only during its
    lifetime.
    """
    def __init__(self):
        self._storage: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory(
            prefix='registry-virtual-', dir=persistent.TMPDIR.name)
        self._artifacts: typing.Dict[str, typing.Dict[
            'lngmod.Version', 'product.Artifact']] = collections.defaultdict(dict)
        super().__init__(self._storage.name)

    def projects(self) -> typing.Iterable[str]:
        return iter(self._artifacts.keys())

    def lineages(self, project: str) -> typing.Iterable[str]:
        return iter(self._artifacts[project].keys())

    def mount(self, project: str, lineage: 'lngmod.Version') -> 'product.Artifact':
        return self._artifacts[project][lineage]

    def pull(self, project: str, lineage: 'lngmod.Version') -> 'distribution.Package':
        raise NotImplementedError('No packages in virtual repository')

    def push(self, package: 'distribution.Package') -> None:
        artifact = package.install(package.path)  # avoid copying by installing to self
        self._artifacts[package.manifest.name][package.manifest.version] = artifact
