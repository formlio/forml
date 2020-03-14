"""
Project steuptools tests.
"""
# pylint: disable=no-self-use

from forml.project import setuptools


def test_upstream():
    """Test our setuptools imports all upstream features.
    """
    assert setuptools.find_packages
