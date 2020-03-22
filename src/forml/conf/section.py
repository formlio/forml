"""
Config section helpers.
"""
import abc
import collections
import configparser
import functools
import re

import typing

from forml import error


def ensure(parser: configparser.ConfigParser, section: str) -> None:
    """Add given section if missing.

    Args:
        parser: instance to ensure the section on
        section: name of the section to be added.
    """
    try:
        parser.add_section(section)
    except configparser.DuplicateSectionError:
        pass


class Meta(abc.ABCMeta):
    """Metaclass for parsed config options.
    """
    FIELDS_REF = 'FIELDS'
    PATTERN_REF = 'PATTERN'
    PATTERN_DEFAULT = r'\s*(\w+)\s*(?:,|$)'

    def __new__(mcs, name: str,
                bases: typing.Tuple[typing.Type],
                namespace: typing.Dict[str, typing.Any]) -> 'Meta':
        pattern = re.compile(namespace.pop(mcs.PATTERN_REF, mcs.PATTERN_DEFAULT))

        class Base(collections.namedtuple(name, namespace.pop(mcs.FIELDS_REF, name))):
            """Tweaking base class.
            """
            @classmethod
            def parse(cls, ref: str) -> typing.Tuple:
                """Get config list for pattern based non-repeated option tokens.
                """
                result: collections.OrderedDict = collections.OrderedDict()
                while ref:
                    match = pattern.match(ref)
                    if not match:
                        raise error.Unexpected('Invalid token (%s): "%s"' % (name, ref))
                    value = cls(*(match.groups() or (match.group(),)))
                    if value in result:
                        raise error.Unexpected('Repeated value (%s): "%s"' % (name, ref))
                    result[value] = value
                    ref = ref[match.end():]
                return tuple(result)

            @classmethod
            @abc.abstractmethod
            def _default(cls) -> tuple:
                """Return the default parsing.
                """
                raise NotImplementedError(f'No defaults for {name}')

        return super().__new__(mcs, name, bases or tuple([Base]), namespace)

    @property
    @functools.lru_cache()
    def default(cls) -> tuple:
        """Convenience "class" property for the default getter.

        Returns: default instance.
        """
        return cls._default()
