"""
ETL schema types.
"""
import abc
import collections
import operator

import typing

if typing.TYPE_CHECKING:
    from forml import etl  # pylint: disable=unused-import; # noqa: F401


class Visitor(metaclass=abc.ABCMeta):
    """Schema visitor.
    """
    def visit_source(self, source: 'Source') -> None:
        """Generic source hook.

        Args:
            source: Source instance to be visited.
        """

    def visit_table(self, table: 'Table') -> None:
        """Generic source hook.

        Args:
            table: Source instance to be visited.
        """
        self.visit_source(table)

    def visit_column(self, column: 'Column') -> None:
        """Generic column hook.

        Args:
            column: Column instance to be visited.
        """


class Source(metaclass=abc.ABCMeta):
    """Source base class.
    """
    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_source(self)


class Table(tuple, Source):
    """Table based source.

    This type can be used either as metaclass or as a base class to inherit from.
    """
    __schema__ = property(operator.itemgetter(0))

    def __new__(mcs, schema: typing.Union[str, typing.Type['etl.Schema']],  # pylint: disable=bad-classmethod-argument
                bases: typing.Optional[typing.Tuple[typing.Type]] = None,
                namespace: typing.Optional[typing.Dict[str, typing.Any]] = None):
        if issubclass(mcs, Table):  # used as metaclass
            if bases:
                bases = (bases[0].__schema__, )
            schema = type(schema, bases, namespace)
        else:
            if bases or namespace:
                raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, [schema])  # used as constructor

    def __getattr__(self, name: str) -> 'Field':
        field: 'etl.Field' = getattr(self.__schema__, name)
        return Field(self.__schema__, field.name or name, field.kind)

    def accept(self, visitor: Visitor) -> None:
        super().accept(visitor)
        visitor.visit_table(self)


class Column(metaclass=abc.ABCMeta):
    """Base class for column types (ie fields or select expressions).
    """
    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Column nme

        Returns: name string.
        """

    @property
    @abc.abstractmethod
    def kind(self):
        """Column type.

        Returns: type.
        """

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_column(self)

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self, alias)

    def __gt__(self, other: typing.Union['Column', int, float, str]) -> 'Condition':
        ...

    def __add__(self, other: typing.Union['Column', int, float]) -> 'Expression':
        ...


class Aliased(collections.namedtuple('Aliased', 'column, name'), Column):
    """Aliased column representation.
    """
    def __new__(cls, column: Column, alias: str):
        return super().__new__(cls, column, alias)

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self.column, alias)

    @property
    def kind(self):
        """Column type.

        Returns: Inner column type.
        """
        return self.column.kind


class Field(collections.namedtuple('Field', 'schema, name, kind'), Column):
    """Schema field class bound to its table schema.
    """
    def __new__(cls, schema: typing.Type['etl.Schema'], name: str, kind: ...):
        return super().__new__(cls, schema, name, kind)


class Expression(Column):  # pylint: disable=abstract-method
    """Base class for expressions.
    """


class Condition(Expression):  # pylint: disable=abstract-method
    """Condition is a boolean expression.
    """
