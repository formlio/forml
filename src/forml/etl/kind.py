"""ETL type classes.
"""
import abc
import typing


class Singleton(abc.ABCMeta):
    """Metaclass for singleton types.
    """
    def __new__(mcs, name: str, bases: typing.Tuple[type], namespace: typing.Dict[str, typing.Any]):
        def singleton(old: typing.Optional[typing.Callable]) -> typing.Callable:  # pylint: disable=unused-argument
            instance = None

            def new(cls, *args, **kwargs) -> typing.Type:
                """Injected class new method ensuring singletons are only created.
                """
                nonlocal instance
                nonlocal old
                if not instance:
                    if not old:
                        old = super(cls, cls).__new__
                    instance = old(cls, *args, **kwargs)
                elif not isinstance(instance, cls):  # subclass calling parent new
                    return old(cls, *args, **kwargs)
                return instance
            return new
        namespace['__new__'] = singleton(namespace.get('__new__'))
        return super().__new__(mcs, name, bases, namespace)


class Data(metaclass=Singleton):
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
    def __gt__(self, other: 'Primitive') -> 'Logical':
        ...


class Logical(Primitive):
    """Logical data type base class.
    """

    def __and__(self, other):
        ...


class Numeric(Primitive):
    """Numeric data type base class.
    """

    def __add__(self, other):
        ...


class Integer(Numeric):
    """Integer data type class.
    """


class Float(Numeric):
    """Float data type class.
    """


class Decimal(Numeric):
    """Decimal data type class.
    """


class Boolean(Logical):
    """Boolean data type class.
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
