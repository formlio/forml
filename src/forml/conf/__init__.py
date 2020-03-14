"""ForML configuration
"""
import argparse
import configparser
import contextlib
import os
import pathlib
import re
import sys
import tempfile
import types
import typing

from forml.conf import section as secmod


@contextlib.contextmanager
def use_default(option: str, value: str) -> typing.Generator:
    """Context manager for temporally setting default option value.
    """
    original = FILE.get(configparser.DEFAULTSECT, option, fallback=None)
    FILE.set(configparser.DEFAULTSECT, option, value)
    yield
    if original is None:
        FILE.remove_option(configparser.DEFAULTSECT, option)
    else:
        FILE.set(configparser.DEFAULTSECT, option, original)


class Provider(metaclass=secmod.Meta):
    """Provider config container.
    """
    PATTERN = r'\s*(\w+)\s*$'
    FIELDS = 'name, kwargs'

    def __new__(cls, group: str, key: str):  # pylint: disable=unused-argument
        section = f'{group}:{key}'
        secmod.ensure(FILE, section)
        kwargs = dict()
        for option, value in FILE.items(section):
            if FILE.remove_option(section, option):  # take only non-default options
                FILE.set(section, option, value)
                kwargs[option] = value
        provider = kwargs.pop(OPT_PROVIDER, key)
        return super().__new__(cls, provider, types.MappingProxyType(kwargs))

    @classmethod
    def parse(cls, spec: str, group: str) -> 'Provider':
        """Resolve the provider config.

        Args:
            spec: Provider alias.
            group: Provider type.

        Returns: Provider config instance.
        """
        return super().parse(spec, group)[0]  # pylint: disable=no-member

    def __hash__(self):
        return hash(self.name)  # pylint: disable=no-member

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.name == self.name  # pylint: disable=no-member


class Registry(Provider):
    """Registry provider.
    """
    @classmethod
    def parse(cls, spec: str) -> 'Provider':
        return super().parse(spec, OPT_REGISTRY.upper())


class Engine(Provider):
    """Engine provider.
    """
    @classmethod
    def parse(cls, spec: str) -> 'Provider':
        return super().parse(spec, OPT_ENGINE.upper())


class Runner(Provider):
    """Runner provider.
    """
    @classmethod
    def parse(cls, spec: str) -> 'Provider':
        return super().parse(spec, OPT_RUNNER.upper())


PRJNAME = re.sub(r'\.[^.]*$', '', pathlib.Path(sys.argv[0]).name)
APPNAME = 'forml'
USRDIR = pathlib.Path(os.getenv(f'{APPNAME.upper()}_HOME', pathlib.Path.home() / f'.{APPNAME}'))
SYSDIR = pathlib.Path('/etc') / APPNAME
TMPDIR = pathlib.Path(tempfile.gettempdir())
APPCFG = 'config.ini'

SECTION_DEFAULT = 'DEFAULT'
SECTION_REGISTRY = 'REGISTRY'
SECTION_ENGINE = 'ENGINE'
SECTION_RUNNER = 'RUNNER'
SECTION_STAGING = 'STAGING'
SECTION_TESTING = 'TESTING'
OPT_LOGCFG = 'log_cfgfile'
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

CLI = argparse.ArgumentParser(add_help=False)
CLI.add_argument('-C', '--config', type=argparse.FileType(), help='additional config file')
CLI.add_argument('-P', '--registry', type=str, default=None, help=f'persistent registry reference')
CLI.add_argument('-R', '--runner', type=str, default=None, help=f'runtime runner reference')
CLI.add_argument('-E', '--engine', type=str, default=None, help=f'IO engine reference')
CLICFG = CLI.parse_known_args()[0]

FILE = configparser.ConfigParser(DEFAULT_OPTIONS)
FILE.optionxform = str   # we need case sensitivity
FILE.read(pathlib.Path(__file__).parent / APPCFG)
SRC = FILE.read(d / APPCFG for d in (SYSDIR, USRDIR))
USRCFG = getattr(CLICFG.config, 'name', None)
if USRCFG:
    SRC.extend(FILE.read(USRCFG))

LOGCFG = FILE.get(SECTION_DEFAULT, OPT_LOGCFG)

# check if all section exist and add them if not so that config.get doesn't fail
for _section in (SECTION_STAGING, SECTION_TESTING):
    secmod.ensure(FILE, _section)


# pylint: disable=no-member
REGISTRY = Registry.parse(CLICFG.registry or FILE.get(SECTION_DEFAULT, OPT_REGISTRY))
ENGINE = Engine.parse(CLICFG.engine or FILE.get(SECTION_DEFAULT, OPT_ENGINE))
RUNNER = Runner.parse(CLICFG.runner or FILE.get(SECTION_DEFAULT, OPT_RUNNER))

TESTING_RUNNER = Runner.parse(FILE.get(SECTION_TESTING, OPT_RUNNER))
