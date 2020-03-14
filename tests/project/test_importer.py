"""
Project importer tests.
"""
# pylint: disable=no-self-use

import pytest

from forml.project import importer, distribution


def test_isolated(project_package: distribution.Package):
    """Isolated importer unit test.
    """
    with pytest.raises(ModuleNotFoundError):
        importer.isolated(project_package.manifest.package)
    importer.isolated(project_package.manifest.package, project_package.path)
