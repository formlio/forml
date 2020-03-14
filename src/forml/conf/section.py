"""
Config section helpers.
"""
import collections
import configparser
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


class Meta(type):
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
            def parse(cls, spec: str, group: str = name) -> typing.Tuple:
                """Get config list for pattern based non-repeated option tokens.
                """
                result: collections.OrderedDict = collections.OrderedDict()
                while spec:
                    match = pattern.match(spec)
                    if not match:
                        raise error.Unexpected('Invalid token (%s): "%s"' % (name, spec))
                    value = cls(group, *(match.groups() or (match.group(),)))
                    if value in result:
                        raise error.Unexpected('Repeated value (%s): "%s"' % (name, spec))
                    result[value] = value
                    spec = spec[match.end():]
                return tuple(result)

        return type.__new__(mcs, name, bases or tuple([Base]), namespace)
