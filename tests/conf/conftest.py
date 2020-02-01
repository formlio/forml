"""
Common fixtures.
"""
import os.path
import argparse
import importlib

from unittest import mock
import pytest


@pytest.fixture(scope='session')
def cfg_file() -> str:
    """Fixture for the test config file.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')


@pytest.fixture(scope='session')
def conf(cfg_file: str):
    """Fixture for the forml.conf module.
    """
    with mock.patch('forml.conf.argparse.ArgumentParser.parse_known_args',
                    return_value=(argparse.Namespace(config=open(cfg_file, mode='r')), [])):
        from forml import conf  # pylint: disable=import-outside-toplevel
        importlib.reload(conf)
        return conf
