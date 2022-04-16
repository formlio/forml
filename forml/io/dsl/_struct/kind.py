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

from .. import _exception


class Meta(abc.ABCMeta):
    """Meta class for all kinds."""

    @property
    def __subkinds__(cls) -> typing.Iterable[type['Any']]:
        """Return all non-abstract sub-classes of given kind class.

        Returns:
            Iterable of all sub-kinds.
        """

        def scan(subs: typing.Iterable[type['Any']]) -> typing.Iterable[type['Any']]:
            """Scan the class subtree of given types.

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

        def new(cls: type['Any']) -> 'Any':
            """Injected class new method ensuring singletons are only created."""
            nonlocal instance
            if not instance:
                instance = object.__new__(cls)
            return instance

        namespace['__new__'] = new
        return super().__new__(mcs, name, bases, namespace)


Native = typing.TypeVar('Native')


class Any(metaclass=Meta):
    """Type base class."""

    @property
    @abc.abstractmethod
    def __type__(self) -> type[Native]:
        """Native python supertype representing this kind.

        Returns:
            Native type.
        """

    @property
    @abc.abstractmethod
    def __cardinality__(self) -> int:
        """Cardinality (relative size) of given kind. Useful to for example distinguish largest subkind of given kind.

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
    def match(cls, kind: 'Any') -> bool:
        """Check given kind is of our type.

        Args:
            kind: Kind to be verified.

        Returns:
            True if instance of our type.
        """
        return isinstance(kind, cls)

    @classmethod
    def ensure(cls, kind: 'Any') -> 'Any':
        """Ensure given kind is of our type.

        Args:
            kind: Kind to be verified.

        Returns:
            Original kind if instance of our type or raising otherwise.
        """
        if not cls.match(kind):
            raise _exception.GrammarError(f'{kind} not an instance of a {cls.__name__}')
        return kind


class Primitive(Any, metaclass=Singleton):  # pylint: disable=abstract-method
    """Primitive data type base class."""

    def __new__(cls, *args, **kwargs):
        """This gets actually overwritten by metaclass."""
        raise AssertionError('Expected to be replaced by metaclass')


class Numeric(Primitive, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Numeric data type base class."""

    __type__ = numbers.Number


class Boolean(Primitive):
    """Boolean data type class."""

    __type__ = bool
    __cardinality__ = 0


class Integer(Numeric):
    """Integer data type class."""

    __type__ = numbers.Integral
    __cardinality__ = 1


class Float(Numeric):
    """Float data type class."""

    __type__ = numbers.Real
    __cardinality__ = 2


class Decimal(Numeric):
    """Decimal data type class."""

    __type__ = decimal.Decimal
    __cardinality__ = 1


class String(Primitive):
    """String data type class."""

    __type__ = str
    __cardinality__ = 1


class Date(Primitive):
    """Date data type class."""

    __type__ = datetime.date
    __cardinality__ = 2


class Timestamp(Date):
    """Timestamp data type class."""

    __type__ = datetime.datetime
    __cardinality__ = 1


class Compound(Any, tuple):
    """Complex data type class."""

    @property
    def __cardinality__(self) -> int:
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
    """Array data type class."""

    element: Any = property(operator.itemgetter(0))
    __type__ = typing.Sequence

    def __new__(cls, element: Any):
        return tuple.__new__(cls, [element])


class Map(Compound):
    """Map data type class."""

    key: Any = property(operator.itemgetter(0))
    value: Any = property(operator.itemgetter(1))
    __type__ = typing.Mapping

    def __new__(cls, key: Any, value: Any):
        return tuple.__new__(cls, [key, value])


class Struct(Compound):
    """Struct data type class."""

    class Element(collections.namedtuple('Element', 'name, kind')):
        """Struct element type."""

        def __eq__(self, other):
            return other.__class__ == self.__class__ and super().__eq__(other)

        def __hash__(self):
            return hash(self.__class__) ^ super().__hash__()

    __type__ = object

    def __new__(cls, **element: Any):
        return tuple.__new__(cls, [cls.Element(n, k) for n, k in element.items()])


def reflect(value: typing.Any) -> Any:
    """Get the type of the provided value.

    Args:
        value: Value to be inspected for type.

    Returns:
        ETL type.
    """

    def same(seq: typing.Iterable[typing.Any]) -> bool:
        """Return true if all elements of a non-empty sequence have same type.

        Args:
            seq: Sequence of elements to check.

        Returns:
            True if all same.
        """
        seq = iter(seq)
        first = type(next(seq))
        return all(isinstance(i, first) for i in seq)

    for primitive in sorted(Primitive.__subkinds__, key=lambda k: k.__cardinality__):
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
