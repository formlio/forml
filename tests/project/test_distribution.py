"""
Project distribution tests.
"""
# pylint: disable=no-self-use
import os
import pathlib

import pytest

from forml.project import distribution


@pytest.fixture(scope='session')
def manifest() -> distribution.Manifest:
    """Manifest fixture.
    """
    return distribution.Manifest('foo', '1.0.dev1', 'bar', baz='baz')


class TestManifest:
    """Manifest unit tests.
    """
    def test_rw(self, tmp_path: pathlib.Path, manifest: distribution.Manifest):
        """Test reading/writing a manifest.
        """
        manifest.write(tmp_path)
        assert distribution.Manifest.read(tmp_path) == manifest

    def test_invalid(self, tmp_path: str):
        """Test invalid manifests.
        """
        path = os.path.join(tmp_path, f'{distribution.Manifest.MODULE}.py')
        os.open(path, os.O_CREAT)
        with pytest.raises(distribution.Error):  # Invalid manifest
            distribution.Manifest.read(tmp_path)
        os.remove(path)
        with pytest.raises(distribution.Error):  # Unknown manifest
            distribution.Manifest.read(tmp_path)


class TestPackage:
    """Package unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source() -> str:
        """Package source fixture.
        """
        return os.path.abspath('project')

    def test_create(self, source: str, tmp_path: pathlib.Path, manifest: distribution.Manifest):
        """Test package creation.
        """
        target = tmp_path / 'testpkg.mpl'
        package = distribution.Package.create(source, manifest, target)
        assert manifest == package.manifest
