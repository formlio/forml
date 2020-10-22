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
import operator
import typing

from forml import error, conf


class Meta(abc.ABCMeta):
    """Metaclass for parsed config options tht adds the itemgetter properties to the class.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]):
        if 'FIELDS' in namespace:
            for index, field in enumerate(namespace.pop('FIELDS')):
                namespace[field] = property(operator.itemgetter(index))
        return super().__new__(mcs, name, (*bases, tuple), namespace)

    @property
    def default(cls) -> 'Resolved':
        """Default parsing.

        Returns: Default resolved config.
        """
        return cls.resolve()


class Resolved(metaclass=Meta):
    """Resolved config base class.

    Implements parser for config referenced based on following concept:

    [INDEX]
    SELECTOR = reference

    [GROUP.reference]
    <params>
    """
    # list of parsed config field names
    FIELDS: typing.Tuple[str] = ('params', )
    # master section containing the references to the particular GROUP sections
    INDEX: str = abc.abstractmethod
    # name of option in INDEX section containing reference(s) to the particular GROUP section
    SELECTOR: str = abc.abstractmethod
    # common name (prefix) of sections referred by SELECTOR
    GROUP: str = abc.abstractmethod

    def __new__(cls, reference: str):
        return super().__new__(cls, cls._extract(reference))

    @classmethod
    def _extract(cls, reference: str) -> typing.Tuple[typing.Any]:
        """Extract the config options given their section reference.

        Args:
            reference: Config section reference.

        Returns: Options extracted from the referred section.
        """
        try:
            return tuple([conf.PARSER[cls.GROUP][reference]])  # pylint: disable=no-member
        except KeyError as err:
            raise error.Missing(f'Config section not found: [{cls.GROUP}.{reference}]') from err

    @classmethod
    def _lookup(cls, reference: str) -> 'Resolved':
        """Create the config instance based on given reference.

        Args:
            reference: Config reference.

        Returns: Config instance.
        """
        return cls(reference)

    @classmethod
    def resolve(cls, reference: typing.Optional[str] = None) -> 'Resolved':
        """Get config list for pattern based non-repeated option tokens.

        Args:
            reference: Config reference.

        Returns: Config instance.
        """
        reference = reference or conf.PARSER.get(cls.INDEX, {}).get(cls.SELECTOR)
        if not reference:
            raise error.Missing(f'No default reference [{cls.INDEX}].{cls.SELECTOR}')
        return cls._lookup(reference)

    def __hash__(self):
        return hash(self.__class__) ^ hash(tuple(sorted(self.params)))  # pylint: disable=no-member

    @abc.abstractmethod
    def __lt__(self, other: 'Resolved') -> bool:
        """Instances need to be comparable to allow for sorting.

        Args:
            other: Right side of the comparison.

        Returns: True if left is less then right.
        """


class Multi(Resolved):  # pylint: disable=abstract-method
    """Resolved section supporting multiple instances.

    [INDEX]
    SELECTOR = [reference1, reference2]

    [GROUP.reference1]
    <params>
    [GROUP.reference2]
    <params>
    """
    @classmethod
    def _lookup(cls, references: typing.Iterable[str]) -> typing.Sequence[Resolved]:
        """Create a sequence of config instances based on given references.

        Args:
            references: Config references.

        Returns: Config instances.
        """
        if isinstance(references, str):
            references = [references]
        return tuple(sorted(cls(r) for r in references))
