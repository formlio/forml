"""ForML configuration
"""
import argparse
import collections
import configparser
import contextlib
import os.path
import re
import sys
import types
import typing


class Error(Exception):
    """Module Error class"""


@contextlib.contextmanager
def use_default(option: str, value: str) -> typing.Generator:
    """Context manager for temporally setting default option value.
    """
    original = CONFIG.get(configparser.DEFAULTSECT, option, fallback=None)
    CONFIG.set(configparser.DEFAULTSECT, option, value)
    yield
    if original is None:
        CONFIG.remove_option(configparser.DEFAULTSECT, option)
    else:
        CONFIG.set(configparser.DEFAULTSECT, option, original)


def ensure_section(section) -> None:
    """Add given section if missing.

    Args:
        section: name of the section to be added.
    """
    try:
        CONFIG.add_section(section)
    except configparser.DuplicateSectionError:
        pass


class SectionMeta(type):
    """Metaclass for parsed config options.
    """
    FIELDS_REF = 'FIELDS'
    PATTERN_REF = 'PATTERN'
    PATTERN_DEFAULT = r'\s*(\w+)\s*(?:,|$)'

    def __new__(mcs, name: str,
                bases: typing.Tuple[typing.Type],
                namespace: typing.Dict[str, typing.Any]) -> 'SectionMeta':
        pattern = re.compile(namespace.pop(mcs.PATTERN_REF, mcs.PATTERN_DEFAULT))

        class Base(collections.namedtuple(name, namespace.pop(mcs.FIELDS_REF, name))):
            """Tweaking base class.
            """
            @classmethod
            def parse(cls, spec: str, group: str = name) -> typing.Tuple:
                """Get config list for pattern based non-repeated option tokens.
                """
                result: collections.OrderedDict = collections.OrderedDict()
                while spec:
                    match = pattern.match(spec)
                    if not match:
                        raise Error('Invalid token (%s): "%s"' % (name, spec))
                    value = cls(group, *(match.groups() or (match.group(),)))
                    if value in result:
                        raise Error('Repeated value (%s): "%s"' % (name, spec))
                    result[value] = value
                    spec = spec[match.end():]
                return tuple(result)

        return type.__new__(mcs, name, (Base, *bases), namespace)


class Provider(metaclass=SectionMeta):
    """Provider config container.
    """
    PATTERN = r'\s*(\w+)\s*$'
    FIELDS = 'key, kwargs'

    def __new__(cls, group: str, key: str):  # pylint: disable=unused-argument
        section = f'{group}:{key}'
        ensure_section(section)
        kwargs = dict()
        for option, value in CONFIG.items(section):
            if CONFIG.remove_option(section, option):  # take only non-default options
                CONFIG.set(section, option, value)
                kwargs[option] = value
        key = kwargs.pop(OPTION_KEY, key)
        return super().__new__(cls, key, types.MappingProxyType(kwargs))

    def __hash__(self):
        return hash(self.key)  # pylint: disable=no-member

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.key == self.key  # pylint: disable=no-member


PRJ_NAME = re.sub(r'\.[^.]*$', '', os.path.basename(sys.argv[0]))
APP_NAME = 'forml'
USR_DIR = os.getenv(f'{APP_NAME.upper()}_HOME', os.path.join(os.path.expanduser('~'), f'.{APP_NAME}'))
SYS_DIR = os.path.join('/etc', APP_NAME)

SECTION_DEFAULT = 'DEFAULT'
SECTION_REGISTRY = 'REGISTRY'
SECTION_TESTING = 'TESTING'
APP_CFGFILE = 'config.ini'
OPTION_LOG_CFGFILE = 'log_cfgfile'
OPTION_KEY = 'key'
OPTION_REGISTRY = 'registry'
OPTION_ENGINE = 'engine'
OPTION_RUNNER = 'runner'

DEFAULT_OPTIONS = {
    OPTION_LOG_CFGFILE: 'logging.ini',
    OPTION_ENGINE: 'devio',
    OPTION_REGISTRY: 'virtual',
    OPTION_RUNNER: 'dask',
}

CLI_PARSER = argparse.ArgumentParser()
CLI_PARSER.add_argument('-C', '--config', type=argparse.FileType(), help='Config file')
CLI_ARGS, _ = CLI_PARSER.parse_known_args()

CONFIG = configparser.ConfigParser(DEFAULT_OPTIONS)
CONFIG.optionxform = str   # we need case sensitivity
CONFIG.read(os.path.join(os.path.dirname(__file__), APP_CFGFILE))
USED_CONFIGS = CONFIG.read((os.path.join(d, APP_CFGFILE) for d in (SYS_DIR, USR_DIR)))
USR_CFGFILE = getattr(CLI_ARGS.config, 'name', None)
if USR_CFGFILE:
    USED_CONFIGS.extend(CONFIG.read(USR_CFGFILE))

LOG_CFGFILE = CONFIG.get(SECTION_DEFAULT, OPTION_LOG_CFGFILE)

# check if all section exist and add them if not so that config.get doesn't fail
for _section in (SECTION_REGISTRY, ):
    ensure_section(_section)


# pylint: disable=no-member
REGISTRY = Provider.parse(CONFIG.get(SECTION_DEFAULT, OPTION_REGISTRY), OPTION_REGISTRY.upper())[0]
ENGINE = Provider.parse(CONFIG.get(SECTION_DEFAULT, OPTION_ENGINE), OPTION_ENGINE.upper())[0]
RUNNER = Provider.parse(CONFIG.get(SECTION_DEFAULT, OPTION_RUNNER), OPTION_RUNNER.upper())[0]

ensure_section(SECTION_TESTING)
TESTING_RUNNER = CONFIG.get(SECTION_TESTING, OPTION_RUNNER)
