# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
