"""ETL type classes.
"""
import abc
import collections
import datetime
import decimal
import functools
import operator
import typing


class Meta(abc.ABCMeta):
    """Metaclass for singleton types.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[type], namespace: typing.Dict[str, typing.Any]):
        instance = None

        def new(cls: typing.Type['Data']) -> 'Data':
            """Injected class new method ensuring singletons are only created.
            """
            nonlocal instance
            if not instance:
                instance = object.__new__(cls)
            return instance

        namespace['__new__'] = new
        return super().__new__(mcs, name, bases, namespace)


class Data(metaclass=abc.ABCMeta):
    """Type base class.
    """
    @abc.abstractmethod
    def __eq__(self, other):
        """Kind equality.
        """

    @abc.abstractmethod
    def __hash__(self):
        """Kind hashcode.
        """


class Primitive(Data, metaclass=Meta):
    """Primitive data type base class.
    """
    def __eq__(self, other):
        return other.__class__ == self.__class__

    def __hash__(self):
        return hash(self.__class__)


class Numeric(Primitive):
    """Numeric data type base class.
    """


class Boolean(Primitive):
    """Boolean data type class.
    """


class Integer(Numeric):
    """Integer data type class.
    """


class Float(Numeric):
    """Float data type class.
    """


class Decimal(Numeric):
    """Decimal data type class.
    """


class String(Primitive):
    """String data type class.
    """


class Date(Primitive):
    """Date data type class.
    """


class Timestamp(Primitive):
    """Timestamp data type class.
    """


class Compound(Data, metaclass=abc.ABCMeta):
    """Complex data type class.
    """


class Array(collections.namedtuple('Array', 'element'), Compound):
    """Array data type class.
    """
    def __eq__(self, other):
        return super().__eq__(other) and other.element == self.element

    def __hash__(self):
        return Compound.__hash__(self) ^ hash(self.element)


class Map(collections.namedtuple('Map', 'key, value'), Compound):
    """Map data type class.
    """
    def __eq__(self, other):
        return super().__eq__(other) and other.key == self.key and other.value == self.value

    def __hash__(self):
        return Compound.__hash__(self) ^ hash(self.key) ^ hash(self.value)


class Struct(tuple, Compound):
    """Struct data type class.
    """
    class Element(collections.namedtuple('Element', 'name, kind')):
        """Struct element type.
        """
        def __eq__(self, other):
            return other.__class__ == self.__class__ and super().__eq__(other)

        def __hash__(self):
            return hash(self.__class__) ^ super().__hash__()

    def __new__(cls, **element: Data):
        return super().__new__(cls, [cls.Element(n, k) for n, k in element.items()])

    def __eq__(self, other):
        return super().__eq__(other) and all(s == o for s, o in zip(self, other))

    def __hash__(self):
        return functools.reduce(operator.xor, self, Compound.__hash__(self))


def inspect(value: typing.Any) -> Data:
    """Get the type of the provided value.

    Args:
        value: Value to be inspected for type.

    Returns: ETL type.
    """
    if isinstance(value, bool):
        return Boolean()
    if isinstance(value, int):
        return Integer()
    if isinstance(value, float):
        return Float()
    if isinstance(value, str):
        return String()
    if isinstance(value, decimal.Decimal):
        return Decimal()
    if isinstance(value, datetime.datetime):
        return Timestamp()
    if isinstance(value, datetime.date):
        return Date()
    raise ValueError(f'Value {value} is of unknown ETL type')
