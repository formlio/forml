"""
Project distribution tests.
"""
# pylint: disable=no-self-use
import os
import tempfile

import pytest

from forml.project import distribution


class TestManifest:
    """Manifest unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def manifest() -> distribution.Manifest:
        """Manifest fixture.
        """
        return distribution.Manifest('foo', '1.0.dev1', 'bar', baz='baz')

    def test_rw(self, manifest: distribution.Manifest):
        """Test reading/writing a manifest.
        """
        with tempfile.TemporaryDirectory() as temp:
            manifest.write(temp)
            assert distribution.Manifest.read(temp) == manifest

    def test_invalid(self):
        """Test invalid manifests.
        """
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, f'{distribution.Manifest.MODULE}.py')
            os.open(path, os.O_CREAT)
            with pytest.raises(distribution.Error):  # Invalid manifest
                distribution.Manifest.read(temp)
            os.remove(path)
            with pytest.raises(distribution.Error):  # Unknown manifest
                distribution.Manifest.read(temp)
