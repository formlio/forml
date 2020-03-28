"""
ForML cli unit tests.
"""
# pylint: disable=no-self-use
import argparse
import importlib
import pathlib
import sys
from unittest import mock

import pytest


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


def test_parse(cfg_file: pathlib.Path):
    """Fixture for the forml.conf module.
    """
    # pylint: disable=import-outside-toplevel
    with mock.patch('forml.cli.argparse.ArgumentParser.parse_known_args',
                    return_value=(argparse.Namespace(config=cfg_file.open('r')), [])):
        from forml import cli
        importlib.reload(cli)

    del sys.modules[cli.__name__]
    from forml import conf
    assert str(cfg_file) in conf.SRC
