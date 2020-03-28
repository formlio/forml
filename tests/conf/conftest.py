"""
Common fixtures.
"""
import configparser
import importlib
import pathlib
import sys
import types
import typing
from unittest import mock

import pytest


@pytest.fixture(scope='session')
def cfg_file() -> pathlib.Path:
    """Fixture for the test config file.
    """
    return pathlib.Path(__file__).parent / 'config.ini'


@pytest.fixture(scope='session')
def conf(cfg_file: pathlib.Path) -> types.ModuleType:
    """Fixture for the forml.conf module.
    """
    class ConfigParser(configparser.ConfigParser):
        """Fake config parser that reads only our config file.
        """
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            super().read([cfg_file])

        def read(self, *_, **__) -> typing.Sequence[str]:
            """Ignore actual readings.
            """
            return [str(cfg_file)]

    # pylint: disable=import-outside-toplevel
    with mock.patch('forml.conf.configparser.ConfigParser', return_value=ConfigParser()):
        from forml import conf
        importlib.reload(conf)
    del sys.modules[conf.__name__]
    return conf
