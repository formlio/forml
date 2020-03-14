"""
Common fixtures.
"""
import argparse
import importlib
import pathlib
from unittest import mock

import pytest


@pytest.fixture(scope='session')
def cfg_file() -> str:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


@pytest.fixture(scope='session')
def conf(cfg_file: str):
    """Fixture for the forml.conf module.
    """
    with mock.patch('forml.conf.argparse.ArgumentParser.parse_known_args',
                    return_value=(argparse.Namespace(
                        config=open(cfg_file, mode='r'),
                        registry=None,
                        engine=None,
                        runner=None), [])):
        from forml import conf  # pylint: disable=import-outside-toplevel
        importlib.reload(conf)
        return conf
