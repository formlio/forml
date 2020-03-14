"""
Project distribution tests.
"""
# pylint: disable=no-self-use
import os
import pathlib

import pytest

from forml import error
from forml.project import distribution


@pytest.fixture(scope='session')
def project_manifest() -> distribution.Manifest:
    """Manifest fixture.
    """
    return distribution.Manifest('foo', '1.0.dev1', 'bar', baz='baz')


class TestManifest:
    """Manifest unit tests.
    """
    def test_rw(self, tmp_path: pathlib.Path, project_manifest):
        """Test reading/writing a manifest.
        """
        project_manifest.write(tmp_path)
        assert distribution.Manifest.read(tmp_path) == project_manifest

    def test_invalid(self, tmp_path: str):
        """Test invalid manifests.
        """
        path = os.path.join(tmp_path, f'{distribution.Manifest.MODULE}.py')
        os.open(path, os.O_CREAT)
        with pytest.raises(error.Invalid):  # Invalid manifest
            distribution.Manifest.read(tmp_path)
        os.remove(path)
        with pytest.raises(error.Missing):  # Unknown manifest
            distribution.Manifest.read(tmp_path)
        with pytest.raises(error.Invalid):
            distribution.Manifest('foo', 'invalid.version', 'project')


class TestPackage:
    """Package unit tests.
    """
    def test_create(self, project_package: distribution.Package, tmp_path: pathlib.Path):
        """Test package creation.
        """
        result = distribution.Package.create(project_package.path, project_package.manifest, tmp_path / 'testpkg.4ml')
        assert result.manifest == project_package.manifest

    def test_install(self, project_package: distribution.Package, tmp_path: pathlib.Path):
        """Package install unit test.
        """
        artifact = project_package.install(tmp_path / 'foo')
        assert artifact.package == project_package.manifest.package
        assert artifact.descriptor
