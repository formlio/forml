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
import operator
import re
import typing

from forml import error, conf


class Meta(abc.ABCMeta):
    """Metaclass for parsed config options tht adds the itemgetter properties to the class.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]):
        if 'FIELDS' in namespace:
            for index, field in enumerate(namespace.pop('FIELDS')):
                namespace[field] = property(operator.itemgetter(index))
        return super().__new__(mcs, name, bases, namespace)

    @property
    def default(cls) -> 'Parsed':
        """Default parsing.

        Returns: Default parsed config.
        """
        return cls.parse()


class Parsed(tuple, metaclass=Meta):
    """Parsed config base class.

    Implements parser for configs referenced based on following concept:

        [REFEREE]
        selector = reference1, reference2

        [SELECTOR:reference1]
        <kwargs>
        [SELECTOR:reference2]
        <kwargs>
    """
    # list of parsed config field names
    FIELDS: typing.Tuple[str] = ('kwargs', )
    # config reference(s) pattern
    PATTERN: typing.Pattern = re.compile(r'\s*(\w+)\s*(?:,|$)')
    # master section containing the references to the particular config sections
    REFEREE: str = abc.abstractmethod
    # name of option in master section containing reference(s) to the particular
    # config sections as well as their name prefix
    SELECTOR: str = abc.abstractmethod

    def __new__(cls, reference: str, *args):
        return super().__new__(cls, cls.extract(reference, *args))

    @classmethod
    def extract(cls, reference: str, *_) -> typing.Tuple[typing.Any]:
        """Extract the config options given their section reference.

        Args:
            reference: Config section reference.

        Returns: Options extracted from the referred section.
        """
        return tuple([conf.PARSER.get(cls.SELECTOR.upper(), {}).get(reference, {})])  # pylint: disable=no-member

    @classmethod
    def parse(cls, references: typing.Optional[str] = None) -> typing.Tuple['Parsed']:
        """Get config list for pattern based non-repeated option tokens.

        Non-repeatability depends on particular implementations of the __hash__/__eq__ methods.
        """
        if not references:
            references = conf.PARSER[cls.REFEREE][cls.SELECTOR]
        result: collections.OrderedDict = collections.OrderedDict()
        while references:
            match = cls.PATTERN.match(references)
            if not match:
                raise error.Unexpected('Invalid token (%s): "%s"' % (cls.__name__, references))
            value = cls(*(match.groups() or (match.group(),)))
            if value in result:
                raise error.Unexpected('Repeated value (%s): "%s"' % (cls.__name__, references))
            result[value] = value
            references = references[match.end():]
        return tuple(sorted(result))

    def __hash__(self):
        return hash(self.__class__) ^ hash(tuple(sorted(self.kwargs)))  # pylint: disable=no-member

    @abc.abstractmethod
    def __lt__(self, other: 'Parsed') -> bool:
        """Instances need to be comparable to allow for sorting.

        Args:
            other: Right side of the comparison.

        Returns: True if left is less then right.
        """


class Single(Parsed):  # pylint: disable=abstract-method
    """Parsed section supporting only single instance.
    """
    PATTERN = re.compile(r'\s*(\w+)\s*$')

    @classmethod
    def parse(cls, reference: typing.Optional[str] = None) -> 'Single':
        """Resolve the referenced config.

        Args:
            reference: Config reference.

        Returns: Config instance.
        """
        return super().parse(reference)[0]
