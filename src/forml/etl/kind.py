"""ETL type classes.
"""
import abc
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


class Data(metaclass=Meta):
    """Type base class.
    """
    def __eq__(self, other):
        return other.__class__ == self.__class__

    def __hash__(self):
        return hash(self.__class__)

    # @classmethod
    # @abc.abstractmethod
    # def cccxd(cls):
    #     """
    #     Returns:
    #
    #     """


class Primitive(Data):
    """Primitive data type base class.
    """


class Boolean(Primitive):
    """Boolean data type class.
    """


class Numeric(Primitive):
    """Numeric data type base class.
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


class Complex(Data):
    """Complex data type class.
    """


class Array(Complex):
    """Array data type class.
    """


class Map(Complex):
    """Map data type class.
    """


class Struct(Complex):
    """Struct data type class.
    """
