"""ForML configuration
"""
import argparse
import collections
import configparser
import contextlib
import json
import os.path
import re
import typing


class Error(Exception):
    """Module Error class"""
    pass


@contextlib.contextmanager
def use_default(option: str, value: str) -> typing.ContextManager:
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
            def parse(cls, spec: str) -> typing.Tuple:
                """Get config list for pattern based non-repeated option tokens.
                """
                result = collections.OrderedDict()
                while spec:
                    match = pattern.match(spec)
                    if not match:
                        raise Error('Invalid token (%s): "%s"' % (name, spec))
                    value = cls(*(match.groups() or (match.group(),)))
                    if value in result:
                        raise Error('Repeated value (%s): "%s"' % (name, spec))
                    result[value] = value
                    spec = spec[match.end():]
                return tuple(result)

        return type.__new__(mcs, name, (Base, *bases), namespace)


class Registry(object, metaclass=SectionMeta):
    """Registry config container.
    """
    PATTERN = r'\s*(\w+)\s*$'
    FIELDS = 'name, cls, kwargs'

    def __new__(cls, name: str, **kwargs) -> 'Registry':
        section = f'{SECTION_REGISTRY}:{name}'
        ensure_section(section)
        # with use_default(OPTION_NAME, name):
        #     name = CONFIG.get(section, OPTION_NAME)
        # return super().__new__(
        #     cls, name, CONFIG.get(section, OPTION_CLASS, vars=kwargs),
        #     json.loads(CONFIG.get(section, OPTION_KWARGS, vars=kwargs)))


APP_NAME = 'forml'
USR_DIR = os.getenv(f'{APP_NAME.upper()}_HOME', os.path.join(os.path.expanduser('~'), f'.{APP_NAME}'))
SYS_DIR = os.path.join('/etc', APP_NAME)

SECTION_DEFAULT = 'DEFAULT'
SECTION_REGISTRY = 'REGISTRY'
OPTION_LOG_CFGFILE = 'log_cfgfile'
OPTION_REGISTRY = 'registry'
OPTION_ENGINE = 'engine'
OPTION_RUNTIME = 'runtime'

DEFAULT_OPTIONS = {
    OPTION_LOG_CFGFILE: 'logging.ini',
    OPTION_REGISTRY: 'local',
    OPTION_RUNTIME: 'dask',
}

CLI_PARSER = argparse.ArgumentParser()
CLI_PARSER.add_argument('-C', '--config', type=argparse.FileType(), help='Config file')
CLI_ARGS, _ = CLI_PARSER.parse_known_args()

APP_CFGFILE = CLI_ARGS.config or 'config.ini'
CONFIG = configparser.ConfigParser(DEFAULT_OPTIONS)
CONFIG.optionxform = str   # we need case sensitivity
USED_CONFIGS = CONFIG.read((os.path.join(d, APP_CFGFILE) for d in (SYS_DIR, USR_DIR)))

LOG_CFGFILE = CONFIG.get(SECTION_DEFAULT, OPTION_LOG_CFGFILE)

# check if all section exist and add them if not so that config.get doesn't fail
# for _section in (SECTION_DEFAULT, ):
#     ensure_section(_section)


REGISTRY = Registry.parse(CONFIG.get(SECTION_DEFAULT, OPTION_REGISTRY))
