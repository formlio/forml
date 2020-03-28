"""ForML configuration
"""
import configparser
import contextlib
import os
import pathlib
import re
import sys
import tempfile
import typing

from forml.conf import section as secmod


@contextlib.contextmanager
def use_default(option: str, value: str) -> typing.Generator:
    """Context manager for temporally setting default option value.
    """
    original = PARSER.get(configparser.DEFAULTSECT, option, fallback=None)
    PARSER.set(configparser.DEFAULTSECT, option, value)
    yield
    if original is None:
        PARSER.remove_option(configparser.DEFAULTSECT, option)
    else:
        PARSER.set(configparser.DEFAULTSECT, option, original)


APPNAME = 'forml'
PRJNAME = re.sub(r'\.[^.]*$', '', pathlib.Path(sys.argv[0]).name)
SYSDIR = pathlib.Path('/etc') / APPNAME
USRDIR = pathlib.Path(os.getenv(f'{APPNAME.upper()}_HOME', pathlib.Path.home() / f'.{APPNAME}'))
PATH = pathlib.Path(__file__).parent, SYSDIR, USRDIR
TMPDIR = pathlib.Path(tempfile.gettempdir())
APPCFG = 'config.ini'

SECTION_DEFAULT = 'DEFAULT'
SECTION_REGISTRY = 'REGISTRY'
SECTION_ENGINE = 'ENGINE'
SECTION_RUNNER = 'RUNNER'
SECTION_STAGING = 'STAGING'
SECTION_TESTING = 'TESTING'
OPT_LOGCFG = 'logcfg'
OPT_PROVIDER = 'provider'
OPT_REGISTRY = 'registry'
OPT_ENGINE = 'engine'
OPT_RUNNER = 'runner'

DEFAULT_OPTIONS = {
    OPT_LOGCFG: 'logging.ini',
    OPT_ENGINE: 'devio',
    OPT_REGISTRY: 'virtual',
    OPT_RUNNER: 'dask',
}


PARSER = configparser.ConfigParser(DEFAULT_OPTIONS)
PARSER.optionxform = str   # we need case sensitivity
SRC = PARSER.read(p / APPCFG for p in PATH)


def get(key: str, section: str = SECTION_DEFAULT, **kwargs) -> str:
    """Get the config value.

    Args:
        section: Config file section.
        key: Option key.

    Returns: Config value
    """
    return PARSER.get(section, key, **kwargs)


# check if all section exist and add them if not so that config.get doesn't fail
for _section in (SECTION_STAGING, SECTION_TESTING):
    secmod.ensure(PARSER, _section)
