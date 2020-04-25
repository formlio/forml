"""
ETL schema types.
"""
import abc
import collections
import operator

import typing

from forml.etl import kind as kindmod
from forml.etl.expression.symbol import comparison

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

    def visit_field(self, field: 'Field') -> None:
        """Generic expression hook.

        Args:
            field: Field instance to be visited.
        """
        self.visit_column(field)

    def visit_lieral(self, literal: 'Literal') -> None:
        """Generic literal hook.

        Args:
            literal: Literal instance to be visited.
        """
        self.visit_column(literal)

    def visit_expression(self, expression: 'Expression') -> None:
        """Generic expression hook.

        Args:
            expression: Expression instance to be visited.
        """
        self.visit_column(expression)


class Source(metaclass=abc.ABCMeta):
    """Source base class.
    """
    @abc.abstractmethod
    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """


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

    @abc.abstractmethod
    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self, alias)

    def __lt__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.LessThan(self, other)

    def __le__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.LessEqual(self, other)

    def __gt__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.GreaterThan(self, other)

    def __ge__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.GreaterEqual(self, other)

    def __eq__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.Equal(self, other)

    def __ne__(self, other: typing.Union['Column', int, float, str]) -> 'Expression':
        return comparison.NotEqual(self, other)

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

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        self.column.accept(visitor)


class Literal(collections.namedtuple('Literal', 'value, kind'), Column):
    @property
    def name(self) -> None:
        """Literal has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_literal(self)


class Field(collections.namedtuple('Field', 'schema, name, kind'), Column):
    """Schema field class bound to its table schema.
    """
    def __new__(cls, schema: typing.Type['etl.Schema'], name: str, kind: kindmod.Data):
        return super().__new__(cls, schema, name, kind)

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_field(self)


class Expression(tuple, Column, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for expressions.
    """
    def __new__(cls, *terms: Column):
        return super().__new__(cls, terms)

    @property
    def name(self) -> None:
        """Expression has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    def accept(self, visitor: Visitor) -> None:
        for term in self:
            term.accept(visitor)
        visitor.visit_expression(self)
