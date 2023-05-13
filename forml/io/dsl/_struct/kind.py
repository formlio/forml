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

"""ETL type classes.
"""
import abc
import collections
import datetime
import decimal
import inspect
import numbers
import operator
import typing

import pandas

from .. import _exception

if typing.TYPE_CHECKING:
    from forml.io import dsl


class Meta(abc.ABCMeta):
    """Meta class for all kinds."""

    @property
    def __subkinds__(cls) -> typing.Iterable[type['dsl.Any']]:
        """Return all non-abstract sub-classes of the given kind class.

        Returns:
            Iterable of all sub-kinds.
        """

        def scan(subs: typing.Iterable[type['dsl.Any']]) -> typing.Iterable[type['dsl.Any']]:
            """Scan the class subtree of the given types.

            Args:
                subs: Iterable of classes to descend from.

            Returns:
                Iterable of all subclasses.
            """
            return (s for c in subs for s in (c, *scan(c.__subclasses__())))

        return {k for k in scan(cls.__subclasses__()) if not inspect.isabstract(k)}


class Singleton(Meta):
    """Metaclass for singleton types."""

    def __new__(mcs, name: str, bases: tuple[type], namespace: dict[str, typing.Any]):
        instance = None

        def new(cls: type['dsl.Any']) -> 'dsl.Any':
            """Singleton type."""
            nonlocal instance
            if not instance:
                instance = object.__new__(cls)
            return instance

        namespace['__new__'] = new
        return super().__new__(mcs, name, bases, namespace)


Native = typing.TypeVar('Native')


class Any(metaclass=Meta):
    """Base class of all types."""

    @property
    @abc.abstractmethod
    def __type__(self) -> type[Native]:
        """Native python supertype representing this kind.

        Returns:
            Native type.
        """

    @property
    @abc.abstractmethod
    def __rank__(self) -> int:
        """Rank (relative size) of the given kind.

        Useful to for example distinguish largest sub-kind of the given kind.

        Returns:
            Cardinality value.
        """

    @abc.abstractmethod
    def __new__(cls, *args, **kwargs):
        """Abstract constructor."""
        raise NotImplementedError()

    def __eq__(self, other):
        return other.__class__ == self.__class__

    def __hash__(self):
        return hash(self.__class__)

    def __repr__(self):
        return self.__class__.__name__

    @classmethod
    def match(cls, kind: 'dsl.Any') -> bool:
        """Check the given kind is of our type.

        Args:
            kind: Kind to be verified.

        Returns:
            True if instance of our type.
        """
        return isinstance(kind, cls)

    @classmethod
    def ensure(cls, kind: 'dsl.Any') -> 'dsl.Any':
        """Ensure the given kind is of our type.

        Args:
            kind: Kind to be verified.

        Returns:
            Original kind if instance of our type or raising otherwise.
        """
        if not cls.match(kind):
            raise _exception.GrammarError(f'{kind} is not a {cls.__name__}')
        return kind

    @classmethod
    def cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        """Cast the value to this kind.

        Args:
            Value to cast.

        Returns:
            Value as this kind.

        Raises:
            dsl.CastError: If casting that value is not possible.
        """
        try:
            return cls._cast(value)
        except (ValueError, TypeError) as err:
            raise _exception.CastError(f'Unable to cast {repr(value)} as {cls.__name__}') from err

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        """Cast the value to this kind.

        Args:
            Value to cast.

        Returns:
            Value as this kind.
        """
        return cls.__type__(value)


class Primitive(Any, metaclass=Singleton):  # pylint: disable=abstract-method
    """Primitive data type base class."""

    def __new__(cls, *args, **kwargs):
        """This gets actually overwritten by metaclass."""
        raise AssertionError('Expected to be replaced by metaclass')

    @classmethod
    @typing.final
    def cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        if isinstance(value, cls.__type__):  # pylint: disable=isinstance-second-argument-not-valid-type
            return value
        return super().cast(value)


class Numeric(Primitive, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Numeric data type base class."""

    __type__ = numbers.Number

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return pandas.to_numeric(value)


class Boolean(Primitive):
    """Boolean data type class."""

    __type__ = bool
    __rank__ = 0


class Integer(Numeric):
    """Integer data type class."""

    __type__ = numbers.Integral
    __rank__ = 1

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return int(value)


class Float(Numeric):
    """Float data type class."""

    __type__ = numbers.Real
    __rank__ = 2

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return float(value)


class Decimal(Numeric):
    """Decimal data type class."""

    __type__ = decimal.Decimal
    __rank__ = 1

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return decimal.Decimal(value)


class String(Primitive):
    """String data type class."""

    __type__ = str
    __rank__ = 1


class Date(Primitive):
    """Date data type class."""

    __type__ = datetime.date
    __rank__ = 2

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return pandas.to_datetime(value).date()


class Timestamp(Date):
    """Timestamp data type class."""

    __type__ = datetime.datetime
    __rank__ = 1

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        return pandas.to_datetime(value)


class Compound(Any, tuple, metaclass=abc.ABCMeta):
    """Complex data type class."""

    @classmethod
    def _cast(cls, value: 'dsl.Native') -> 'dsl.Native':
        raise _exception.UnsupportedError('Compound value casting not implemented.')

    @property
    def __rank__(self) -> int:
        return len(self)

    @abc.abstractmethod
    def __new__(cls, *args, **kwargs):
        """Abstract constructor."""
        raise NotImplementedError()

    def __eq__(self, other):
        return Any.__eq__(self, other) and tuple.__eq__(self, other)

    def __hash__(self):
        return Any.__hash__(self) ^ tuple.__hash__(self)


class Array(Compound):
    """Array data type class.

    Args:
        element: Array element kind.
    """

    element: 'dsl.Any' = property(operator.itemgetter(0))
    __type__ = typing.Sequence

    def __new__(cls, element: 'dsl.Any'):
        return tuple.__new__(cls, [element])


class Map(Compound):
    """Map data type class.

    Args:
        key: Map keys kind.
        value: Map values kind.
    """

    key: 'dsl.Any' = property(operator.itemgetter(0))
    value: 'dsl.Any' = property(operator.itemgetter(1))
    __type__ = typing.Mapping

    def __new__(cls, key: 'dsl.Any', value: 'dsl.Any'):
        return tuple.__new__(cls, [key, value])


class Struct(Compound):
    """Structure data type class.

    Args:
        element: Mapping of attribute name strings and their kinds.
    """

    class Element(collections.namedtuple('Element', 'name, kind')):
        """Struct element type."""

        def __eq__(self, other):
            return other.__class__ == self.__class__ and super().__eq__(other)

        def __hash__(self):
            return hash(self.__class__) ^ super().__hash__()

    __type__ = object

    def __new__(cls, **element: 'dsl.Any'):
        return tuple.__new__(cls, [cls.Element(n, k) for n, k in element.items()])


def reflect(value: typing.Any) -> 'dsl.Any':
    """Get the type of the provided value.

    Args:
        value: Value to be inspected for type.

    Returns:
        ETL type.
    """

    def same(seq: typing.Iterable[typing.Any]) -> bool:
        """Return true if all elements of a non-empty sequence have the same type.

        Args:
            seq: Sequence of elements to check.

        Returns:
            True if all same.
        """
        seq = iter(seq)
        first = type(next(seq))
        return all(isinstance(i, first) for i in seq)

    for primitive in sorted(Primitive.__subkinds__, key=lambda k: k.__rank__):
        if isinstance(value, primitive.__type__):
            return primitive()
    if value:
        if isinstance(value, typing.Sequence):
            return Array(reflect(value[0]))
        if isinstance(value, typing.Mapping):
            keys = tuple(value.keys())
            vals = tuple(value.values())
            if same(keys):
                ktype = reflect(keys[0])
                if same(vals):
                    return Map(ktype, reflect(vals[0]))
                if ktype == String():
                    return Struct(**{k: reflect(v) for k, v in value.items()})
    raise ValueError(f'Value {value} is of unknown ETL type')
