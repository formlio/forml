"""
ForML cli unit tests.
"""
# pylint: disable=no-self-use
import argparse
import importlib
import pathlib
from unittest import mock

import pytest

from forml import conf


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


def test_parse(cfg_file: pathlib.Path):
    """Fixture for the forml.conf module.
    """
    with mock.patch('forml.cli.argparse.ArgumentParser.parse_known_args',
                    return_value=(argparse.Namespace(config=cfg_file.open('r')), [])):
        from forml import cli  # pylint: disable=import-outside-toplevel
        importlib.reload(cli)

    assert str(cfg_file) in conf.SRC
