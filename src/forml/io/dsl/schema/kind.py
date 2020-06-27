"""ETL type classes.
"""
import abc
import collections
import datetime
import decimal
import inspect
import operator
import typing
from collections import abc as colabc


class Meta(abc.ABCMeta):
    """Meta class for all kinds.
    """
    @property
    def __subkinds__(cls) -> typing.Iterable[typing.Type['Data']]:
        """Return all non-abstract sub-classes of given kind class.

        Returns: Iterable of all sub-kinds.
        """
        def scan(subs: typing.Iterable[typing.Type['Data']]) -> typing.Iterable[typing.Type['Data']]:
            """Scan the class subtree of given types.

            Args:
                subs: Iterable of classes to descend from.

            Returns: Iterable of all subclasses.
            """
            return (s for c in subs for s in (c, *scan(c.__subclasses__())))
        return {k for k in scan(cls.__subclasses__()) if not inspect.isabstract(k)}


class Singleton(Meta):
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


Native = typing.TypeVar('Native')


class Data(metaclass=Meta):
    """Type base class.
    """

    @property
    @abc.abstractmethod
    def __native__(self) -> Native:
        """Native python type representing this kind.

        Returns: Native type.
        """

    @property
    @abc.abstractmethod
    def __cardinality__(self) -> int:
        """Cardinality (relative size) of give kind. Useful to for example distinguish largest subkind of given kind.

        Returns: Cardinality value.
        """

    @abc.abstractmethod
    def __new__(cls, *args, **kwargs):
        """Abstract constructor.
        """
        raise NotImplementedError()

    def __eq__(self, other):
        return other.__class__ == self.__class__

    def __hash__(self):
        return hash(self.__class__)


class Primitive(Data, metaclass=Singleton):  # pylint: disable=abstract-method
    """Primitive data type base class.
    """
    def __new__(cls, *args, **kwargs):
        """This gets actually overwritten by metaclass.
        """
        assert False, 'Expected to be replaced by metaclass'


class Numeric(Primitive):  # pylint: disable=abstract-method
    """Numeric data type base class.
    """


class Boolean(Primitive):
    """Boolean data type class.
    """
    __native__ = bool
    __cardinality__ = 2


class Integer(Numeric):
    """Integer data type class.
    """
    __native__ = int
    __cardinality__ = 1


class Float(Numeric):
    """Float data type class.
    """
    __native__ = float
    __cardinality__ = 2


class Decimal(Numeric):
    """Decimal data type class.
    """
    __native__ = decimal.Decimal
    __cardinality__ = 3


class String(Primitive):
    """String data type class.
    """
    __native__ = str
    __cardinality__ = 1


class Date(Primitive):
    """Date data type class.
    """
    __native__ = datetime.date
    __cardinality__ = 1


class Timestamp(Date):
    """Timestamp data type class.
    """
    __native__ = datetime.datetime
    __cardinality__ = 2


class Compound(Data, tuple):
    """Complex data type class.
    """
    @property
    def __cardinality__(self) -> int:
        return len(self)

    @abc.abstractmethod
    def __new__(cls, *args, **kwargs):
        """Abstract constructor.
        """
        raise NotImplementedError()

    def __eq__(self, other):
        return Data.__eq__(self, other) and tuple.__eq__(self, other)

    def __hash__(self):
        return Data.__hash__(self) ^ tuple.__hash__(self)


class Array(Compound):
    """Array data type class.
    """
    element: Data = property(operator.itemgetter(0))
    __native__ = list

    def __new__(cls, element: Data):
        return tuple.__new__(cls, [element])


class Map(Compound):
    """Map data type class.
    """
    key: Data = property(operator.itemgetter(0))
    value: Data = property(operator.itemgetter(1))
    __native__ = dict

    def __new__(cls, key: Data, value: Data):
        return tuple.__new__(cls, [key, value])


class Struct(Compound):
    """Struct data type class.
    """
    class Element(collections.namedtuple('Element', 'name, kind')):
        """Struct element type.
        """
        def __eq__(self, other):
            return other.__class__ == self.__class__ and super().__eq__(other)

        def __hash__(self):
            return hash(self.__class__) ^ super().__hash__()

    __native__ = object

    def __new__(cls, **element: Data):
        return tuple.__new__(cls, [cls.Element(n, k) for n, k in element.items()])


def reflect(value: typing.Any) -> Data:
    """Get the type of the provided value.

    Args:
        value: Value to be inspected for type.

    Returns: ETL type.
    """
    def same(seq: typing.Iterable[typing.Any]) -> bool:
        """Return true if all elements of a non-empty sequence have same type.

        Args:
            seq: Sequence of elements to check.

        Returns: True if all same.
        """
        seq = iter(seq)
        first = type(next(seq))
        return all(isinstance(i, first) for i in seq)

    for primitive in sorted(Primitive.__subkinds__, key=lambda k: k.__cardinality__, reverse=True):
        if isinstance(value, primitive.__native__):
            return primitive()
    if value:
        if isinstance(value, colabc.Sequence):
            return Array(reflect(value[0]))
        if isinstance(value, colabc.Mapping):
            keys = tuple(value.keys())
            vals = tuple(value.values())
            if same(keys):
                ktype = reflect(keys[0])
                if same(vals):
                    return Map(ktype, reflect(vals[0]))
                if ktype == String():
                    return Struct(**{k: reflect(v) for k, v in value.items()})
    raise ValueError(f'Value {value} is of unknown ETL type')
