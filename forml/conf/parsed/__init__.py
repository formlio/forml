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
import types
import typing

import forml
from forml import conf


class Meta(abc.ABCMeta):
    """Metaclass for parsed config options tht adds the itemgetter properties to the class."""

    def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
        if 'FIELDS' in namespace:
            for index, field in enumerate(namespace.pop('FIELDS')):
                namespace[field] = property(operator.itemgetter(index))
        return super().__new__(mcs, name, (*bases, tuple), namespace)

    @property
    def default(cls) -> 'Section':
        """Default parsing.

        Returns:
            Default resolved config.
        """
        return cls.resolve()


class Section(metaclass=Meta):
    """Resolved config base class.

    Implements parser for config referenced based on following concept:

    [INDEX]
    SELECTOR = reference

    [GROUP.reference]
    <semantic_param> = <value>  # explicit params consumed by the config parser
    <generic_param> = <value>   # generic params not known to the config parser (ie downstream library config)
    params = { <generic_param> = <value> }  # alternative way of providing generic params to avoid collisions
    """

    # list of parsed config field names
    FIELDS: tuple[str] = ('params',)
    # master section containing the references to the particular GROUP sections
    INDEX: str = abc.abstractmethod
    # name of option in INDEX section containing reference(s) to the particular GROUP section
    SELECTOR: str = abc.abstractmethod
    # common name (prefix) of sections referred by SELECTOR
    GROUP: str = abc.abstractmethod

    def __new__(cls, reference: str):
        try:
            kwargs = conf.PARSER[cls.GROUP][reference]  # pylint: disable=no-member
        except KeyError as err:
            raise forml.MissingError(f'Config section not found: [{cls.GROUP}.{reference}]') from err
        args, kwargs = cls._extract(reference, kwargs)
        return super().__new__(cls, [*args, types.MappingProxyType(dict(kwargs))])

    @classmethod
    def _extract(
        cls, reference: str, kwargs: typing.Mapping[str, typing.Any]  # pylint: disable=unused-argument
    ) -> tuple[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]:
        """Extract the config values as a sequence of "known" semantic arguments and mapping of "generic" options.

        Args:
            reference: Config reference.
            kwargs: Common mapping of values mixing the "known" and "generic".

        Returns:
            Tuple of known plus generic arguments.
        """
        kwargs = dict(kwargs)
        kwargs.update(kwargs.pop(conf.OPT_PARAMS, {}))
        return [], kwargs

    @classmethod
    def _lookup(cls, reference: str) -> 'Section':
        """Create the config instance based on given reference.

        Args:
            reference: Config reference.

        Returns:
            Config instance.
        """
        return cls(reference)

    @classmethod
    def resolve(cls, reference: typing.Optional[str] = None) -> 'Section':
        """Get config list for pattern based non-repeated option tokens.

        Args:
            reference: Config reference.

        Returns:
            Config instance.
        """
        reference = reference or conf.PARSER.get(cls.INDEX, {}).get(cls.SELECTOR)
        if not reference:
            raise forml.MissingError(f'No default reference [{cls.INDEX}].{cls.SELECTOR}')
        return cls._lookup(reference)

    def __hash__(self):
        return hash(self.__class__) ^ hash(tuple(sorted(self.params)))  # pylint: disable=no-member

    @abc.abstractmethod
    def __lt__(self, other: 'Section') -> bool:
        """Instances need to be comparable to allow for sorting.

        Args:
            other: Right side of the comparison.

        Returns:
            True if left is less than right.
        """


class Multi(Section):  # pylint: disable=abstract-method
    """Resolved section supporting multiple instances.

    [INDEX]
    SELECTOR = [reference1, reference2]

    [GROUP.reference1]
    <params>
    [GROUP.reference2]
    <params>
    """

    @classmethod
    def _lookup(cls, reference: typing.Iterable[str]) -> typing.Sequence[Section]:
        """Create a sequence of config instances based on given references.

        Args:
            reference: Config references.

        Returns:
            Config instances.
        """
        if isinstance(reference, str):
            reference = [reference]
        return tuple(sorted(cls(r) for r in reference))
