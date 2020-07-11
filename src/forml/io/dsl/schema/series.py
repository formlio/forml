"""
ETL schema types.
"""
import abc
import functools
import logging
import operator
import typing

from forml.io.dsl.schema import kind as kindmod

if typing.TYPE_CHECKING:
    from forml.io.dsl.schema import frame

LOGGER = logging.getLogger(__name__)


class Visitor(metaclass=abc.ABCMeta):
    """Schema visitor.
    """
    @abc.abstractmethod
    def visit_column(self, column: 'Column') -> None:
        """Generic column hook.

        Args:
            column: Column instance to be visited.
        """

    def visit_aliased(self, aliased: 'Aliased') -> None:
        """Generic expression hook.

        Args:
            aliased: Aliased column instance to be visited.
        """
        self.visit_column(aliased)

    def visit_field(self, field: 'Field') -> None:
        """Generic expression hook.

        Args:
            field: Field instance to be visited.
        """
        self.visit_column(field)

    def visit_literal(self, literal: 'Literal') -> None:
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


def columnize(handler: typing.Callable[['Column'], typing.Any]) -> typing.Callable[[typing.Any], typing.Any]:
    """Decorator for forcing function arguments to columns.

    Args:
        handler: Callable to be decorated.

    Returns: Decorated callable.
    """

    def cast(value: typing.Any) -> 'Column':
        """Attempt to create a literal instance of the value unless already a column.

        Args:
            value: Column to be.

        Returns: Column instance.
        """
        if not isinstance(value, Column):
            LOGGER.debug('Converting value of %s to a literal type', value)
            value = Literal(value)
        return value

    @functools.wraps(handler)
    def wrapper(*args: typing.Any) -> typing.Sequence['Column']:
        """Actual decorator.

        Args:
            *args: Arguments to be forced to columns.

        Returns: Arguments converted to columns.
        """
        return handler(*(cast(a) for a in args))

    return wrapper


class Column(tuple, metaclass=abc.ABCMeta):
    """Base class for column types (ie fields or select expressions).
    """
    class Disect(Visitor):
        """Visitor extracting column elements of given type(s).
        """
        def __init__(self, *types: typing.Type['Column']):
            self._types: typing.FrozenSet[typing.Type['Column']] = frozenset(types)
            self._terms: typing.Set['Column'] = set()

        @property
        def terms(self) -> typing.AbstractSet['Column']:
            """Extracted terms.

            Returns: Set of extracted column terms.
            """
            return frozenset(self._terms)

        def visit_column(self, column: 'Column') -> None:
            if type(column) in self._types:
                self._terms.add(column)

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

    @property
    @abc.abstractmethod
    def element(self) -> 'Element':
        """Return the element of this column (apart from Aliased, element is the column itself).

        Returns: Column's element.
        """

    @classmethod
    def disect(cls, column: 'Column') -> typing.AbstractSet['Column']:
        """Return an iterable of instances of this type composing given column.

        Returns: Set of this type instances used in given column.
        """
        disector = cls.Disect(cls)
        column.accept(disector)
        return disector.terms

    @classmethod
    def ensure(cls, column: 'Column') -> 'Column':
        """Ensure given given column is of our type.
        """
        if not isinstance(column, cls):
            raise ValueError(f'{column} not a {cls.__name__}')
        return column

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self, alias)

    def __hash__(self):
        return hash(self.__class__) ^ tuple.__hash__(self)

    @columnize
    def __lt__(self, other: 'Column') -> 'Expression':
        return LessThan(self, other)

    @columnize
    def __le__(self, other: 'Column') -> 'Expression':
        return LessEqual(self, other)

    @columnize
    def __gt__(self, other: 'Column') -> 'Expression':
        return GreaterThan(self, other)

    @columnize
    def __ge__(self, other: 'Column') -> 'Expression':
        return GreaterEqual(self, other)

    @columnize
    def __eq__(self, other: 'Column') -> 'Expression':
        return Equal(self, other)

    @columnize
    def __ne__(self, other: 'Column') -> 'Expression':
        return NotEqual(self, other)

    @columnize
    def __and__(self, other: 'Column') -> 'Expression':
        return And(self, other)

    @columnize
    def __rand__(self, other: 'Column') -> 'Expression':
        return And(other, self)

    @columnize
    def __or__(self, other: 'Column') -> 'Expression':
        return Or(self, other)

    @columnize
    def __ror__(self, other: 'Column') -> 'Expression':
        return Or(other, self)

    def __invert__(self) -> 'Expression':
        return Not(self)

    @columnize
    def __add__(self, other: 'Column') -> 'Expression':
        return Addition(self, other)

    @columnize
    def __radd__(self, other: 'Column') -> 'Expression':
        return Addition(other, self)

    @columnize
    def __sub__(self, other: 'Column') -> 'Expression':
        return Subtraction(self, other)

    @columnize
    def __rsub__(self, other: 'Column') -> 'Expression':
        return Subtraction(other, self)

    @columnize
    def __mul__(self, other: 'Column') -> 'Expression':
        return Multiplication(self, other)

    @columnize
    def __rmul__(self, other: 'Column') -> 'Expression':
        return Multiplication(other, self)

    @columnize
    def __truediv__(self, other: 'Column') -> 'Expression':
        return Division(self, other)

    @columnize
    def __rtruediv__(self, other: 'Column') -> 'Expression':
        return Division(other, self)

    @columnize
    def __mod__(self, other: 'Column') -> 'Expression':
        return Modulus(self, other)

    @columnize
    def __rmod__(self, other: 'Column') -> 'Expression':
        return Modulus(other, self)


class Aliased(Column):
    """Aliased column representation.
    """
    column: Column = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, column: Column, alias: str):
        return super().__new__(cls, [column, alias])

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias: new alias value.

        Returns: New column instance with given alias.
        """
        return Aliased(self.column, alias)

    @property
    def element(self) -> 'Element':
        return self.column.element

    @property
    def kind(self) -> kindmod.Any:
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
        visitor.visit_aliased(self)


class Element(Column, metaclass=abc.ABCMeta):
    """Base class for any non-aliased columns.
    """
    @property
    def element(self) -> 'Element':
        return self


class Literal(Element):
    """Literal value.
    """
    value: typing.Any = property(operator.itemgetter(0))
    kind: kindmod.Any = property(operator.itemgetter(1))

    def __new__(cls, value: typing.Any):
        return super().__new__(cls, [value, kindmod.reflect(value)])

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


class Field(Element):
    """Schema field class bound to its table schema.
    """
    table: typing.Type['frame.Table'] = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))
    kind: kindmod.Any = property(operator.itemgetter(2))

    def __new__(cls, table: typing.Type['frame.Table'], name: str, kind: kindmod.Any):
        return super().__new__(cls, [table, name, kind])

    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_field(self)


class Expression(Element, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
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
            if isinstance(term, Column):
                term.accept(visitor)
        visitor.visit_expression(self)


class Univariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of just one argument/operand.
    """
    def __new__(cls, arg: Column):
        return super().__new__(cls, arg)


class Bivariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands.
    """
    def __new__(cls, arg1: Column, arg2: Column):
        return super().__new__(cls, arg1, arg2)


class Logical:
    """Mixin for logical functions/operators.
    """
    kind = kindmod.Boolean()

    @classmethod
    def ensure(cls, column: Column) -> Column:
        """Ensure given expression is a logical one.
        """
        if not isinstance(column.kind, cls.kind.__class__):
            raise ValueError(f'{column.kind} not a valid {cls.kind}')
        return column


class LessThan(Logical, Bivariate):
    """Less-Than operator.
    """


class LessEqual(Logical, Bivariate):
    """Less-Equal operator.
    """


class GreaterThan(Logical, Bivariate):
    """Greater-Than operator.
    """


class GreaterEqual(Logical, Bivariate):
    """Greater-Equal operator.
    """


class Equal(Logical, Bivariate):
    """Equal operator.
    """
    def __bool__(self):
        """Since this instance is also returned when python internally compares two Column instances for equality, we
        want to evaluate the boolean value for python perspective of the objects (rather than just the ETL perspective
        of the data).

        Note this doesn't reflect mathematical commutativity - order of potential sub-expression operands matters.
        """
        return hash(self[0]) == hash(self[1])


class NotEqual(Logical, Bivariate):
    """Not-Equal operator.
    """


class IsNull(Logical, Bivariate):
    """Is-Null operator.
    """


class NotNull(Logical, Bivariate):
    """Not-Null operator.
    """


class And(Logical, Bivariate):
    """And operator.
    """


class Or(Logical, Bivariate):
    """Or operator.
    """


class Not(Logical, Univariate):
    """Not operator.
    """


class Arithmetic:
    """Mixin for numerical functions/operators.
    """
    @property
    def kind(self) -> kindmod.Numeric:
        """Largest cardinality kind of all operators kinds.

        Returns: Numeric kind.
        """
        return functools.reduce(functools.partial(max, key=lambda k: k.__cardinality__),
                                (o.kind for o in self))  # pylint: disable=not-an-iterable


class Addition(Arithmetic, Bivariate):
    """Plus operator.
    """


class Subtraction(Arithmetic, Bivariate):
    """Minus operator.
    """


class Multiplication(Arithmetic, Bivariate):
    """Times operator.
    """


class Division(Arithmetic, Bivariate):
    """Divide operator.
    """


class Modulus(Arithmetic, Bivariate):
    """Modulus operator.
    """
