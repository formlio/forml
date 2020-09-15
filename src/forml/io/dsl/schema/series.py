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
ETL schema types.
"""
import abc
import collections
import enum
import functools
import itertools
import logging
import operator
import typing
from collections import abc as colabc

from forml.io.dsl.schema import kind as kindmod, visit

if typing.TYPE_CHECKING:
    from forml.io.dsl.schema import frame as framod

LOGGER = logging.getLogger(__name__)


def cast(value: typing.Any) -> 'Column':
    """Attempt to create a literal instance of the value unless already a column.

    Args:
        value: Value to be represented as Element column.

    Returns: Element column instance.
    """
    if not isinstance(value, Column):
        LOGGER.debug('Converting value of %s to a literal type', value)
        value = Literal(value)
    return value


class Column(tuple, metaclass=abc.ABCMeta):
    """Base class for column types (ie fields or select expressions).
    """
    class Dissect(visit.Series):
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

        def visit_source(self, source: 'framod.Source') -> None:
            """Do nothing for source types.
            """

        def visit_column(self, column: 'Column') -> None:
            if any(isinstance(column, t) for t in self._types):
                self._terms.add(column)

    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    @property
    @abc.abstractmethod
    def name(self) -> typing.Optional[str]:
        """Column nme

        Returns: name string.
        """

    @property
    @abc.abstractmethod
    def kind(self) -> kindmod.Any:
        """Column type.

        Returns: type.
        """

    @abc.abstractmethod
    def accept(self, visitor: visit.Series) -> None:
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
    def dissect(cls, *column: 'Column') -> typing.AbstractSet['Column']:
        """Return an iterable of instances of this type composing given column(s).

        Returns: Set of this type instances used in given column(s).
        """
        def visit(subject: 'Column') -> typing.Iterable['Column']:
            """Dissect a single column.

            Args:
                subject: Column to be dissected.

            Returns: Dissected column instances.
            """
            dissector = cls.Dissect(cls)
            subject.accept(dissector)
            return dissector.terms
        return {t for c in column for t in visit(c)}

    @classmethod
    def ensure(cls, column: 'Column') -> 'Column':
        """Ensure given given column is of our type.
        """
        column = cast(column)
        if not isinstance(column, cls):
            raise ValueError(f'{column} not a {cls.__name__}')
        return column


def columnize(handler: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
    """Decorator for forcing function arguments to element columns.

    Args:
        handler: Callable to be decorated.

    Returns: Decorated callable.
    """
    @functools.wraps(handler)
    def wrapper(*args: typing.Any) -> typing.Sequence['Element']:
        """Actual decorator.

        Args:
            *args: Arguments to be forced to columns.

        Returns: Arguments converted to columns.
        """
        return handler(*(cast(a).element for a in args))

    return wrapper


class Element(Column, metaclass=abc.ABCMeta):
    """Base class for columns that can be used in expressions, conditions, grouping and/or ordering definitions.
    """
    @property
    def element(self) -> 'Element':
        return self

    @classmethod
    def ensure(cls, column: 'Column') -> 'Element':
        """Ensure given given column is an Element.
        """
        return super().ensure(column).element

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self, alias)

    __hash__ = Column.__hash__  # otherwise gets overwritten to None due to redefined __eq__

    @columnize
    def __eq__(self, other: 'Element') -> 'Expression':
        return Equal(self, other)

    @columnize
    def __ne__(self, other: 'Element') -> 'Expression':
        return NotEqual(self, other)

    @columnize
    def __lt__(self, other: 'Element') -> 'Expression':
        return LessThan(self, other)

    @columnize
    def __le__(self, other: 'Element') -> 'Expression':
        return LessEqual(self, other)

    @columnize
    def __gt__(self, other: 'Element') -> 'Expression':
        return GreaterThan(self, other)

    @columnize
    def __ge__(self, other: 'Element') -> 'Expression':
        return GreaterEqual(self, other)

    @columnize
    def __and__(self, other: 'Element') -> 'Expression':
        return And(self, other)

    @columnize
    def __rand__(self, other: 'Element') -> 'Expression':
        return And(other, self)

    @columnize
    def __or__(self, other: 'Element') -> 'Expression':
        return Or(self, other)

    @columnize
    def __ror__(self, other: 'Element') -> 'Expression':
        return Or(other, self)

    def __invert__(self) -> 'Expression':
        return Not(self)

    @columnize
    def __add__(self, other: 'Element') -> 'Expression':
        return Addition(self, other)

    @columnize
    def __radd__(self, other: 'Element') -> 'Expression':
        return Addition(other, self)

    @columnize
    def __sub__(self, other: 'Element') -> 'Expression':
        return Subtraction(self, other)

    @columnize
    def __rsub__(self, other: 'Element') -> 'Expression':
        return Subtraction(other, self)

    @columnize
    def __mul__(self, other: 'Element') -> 'Expression':
        return Multiplication(self, other)

    @columnize
    def __rmul__(self, other: 'Element') -> 'Expression':
        return Multiplication(other, self)

    @columnize
    def __truediv__(self, other: 'Element') -> 'Expression':
        return Division(self, other)

    @columnize
    def __rtruediv__(self, other: 'Element') -> 'Expression':
        return Division(other, self)

    @columnize
    def __mod__(self, other: 'Element') -> 'Expression':
        return Modulus(self, other)

    @columnize
    def __rmod__(self, other: 'Element') -> 'Expression':
        return Modulus(other, self)


class Ordering(collections.namedtuple('Ordering', 'column, direction')):
    """OrderBy spec.
    """
    @enum.unique
    class Direction(enum.Enum):
        """Ordering direction.
        """
        ASCENDING = 'ascending'
        DESCENDING = 'descending'

        def __call__(self, column: typing.Union[Element, 'Ordering']) -> 'Ordering':
            if isinstance(column, Ordering):
                column = column.column
            return Ordering(column, self)

    def __new__(cls, column: Element,
                direction: typing.Optional[typing.Union['Ordering.Direction', str]] = None):
        return super().__new__(cls, Element.ensure(column),
                               cls.Direction(direction) if direction else cls.Direction.ASCENDING)

    @classmethod
    def make(cls, specs: typing.Sequence[typing.Union[Element, typing.Union[
        'Ordering.Direction', str], typing.Tuple[Element, typing.Union[
            'Ordering.Direction', str]]]]) -> typing.Iterable['Ordering']:
        """Helper to generate orderings from given columns and directions.

        Args:
            specs: One or many columns or actual ordering instances.

        Returns: Sequence of ordering terms.
        """
        specs = itertools.zip_longest(specs, specs[1:])
        for column, direction in specs:
            if isinstance(column, Element):
                if isinstance(direction, (Ordering.Direction, str)):
                    yield Ordering.Direction(direction)(column)
                    next(specs)  # pylint: disable=stop-iteration-return
                else:
                    yield Ordering(column)
            elif isinstance(column, colabc.Sequence) and len(column) == 2:
                column, direction = column
                yield Ordering.Direction(direction)(column)
            else:
                raise ValueError('Expecting pair of column and direction')


class Aliased(Column):
    """Aliased column representation.
    """
    element: Element = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, column: Column, alias: str):
        return super().__new__(cls, [column.element, alias])

    @property
    def kind(self) -> kindmod.Any:
        """Column type.

        Returns: Inner column type.
        """
        return self.element.kind

    def accept(self, visitor: visit.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        self.element.accept(visitor)
        visitor.visit_aliased(self)


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

    def accept(self, visitor: visit.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_literal(self)


class Field(Element):
    """Schema field class bound to its table schema.
    """
    source: 'framod.Source' = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, table: 'framod.Tangible', name: str):
        return super().__new__(cls, [table, name])

    @property
    def kind(self) -> kindmod.Any:
        return self.source.schema[self.name].kind

    def accept(self, visitor: visit.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        self.source.accept(visitor)
        visitor.visit_field(self)


class Expression(Element, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for expressions.
    """
    def __new__(cls, *terms: Element):
        return super().__new__(cls, terms)

    @property
    def name(self) -> None:
        """Expression has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    def accept(self, visitor: visit.Series) -> None:
        for term in self:
            if isinstance(term, Element):
                term.accept(visitor)
        visitor.visit_expression(self)


class Univariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of just one argument/operand.
    """
    def __new__(cls, arg: Element):
        return super().__new__(cls, arg)


class Bivariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands.
    """
    def __new__(cls, arg1: Element, arg2: Element):
        return super().__new__(cls, arg1, arg2)


class Logical:
    """Mixin for logical functions/operators.
    """
    kind = kindmod.Boolean()

    @classmethod
    def ensure(cls, column: Element) -> Element:
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


class Multirow(Expression, metaclass=abc.ABCMeta):
    """Base class for expressions involving cross-row operations.
    """


class Window(Multirow):
    """Window type column representation.
    """
    function: 'Window.Function' = property(operator.itemgetter(0))
    partition: typing.Tuple[Element] = property(operator.itemgetter(1))
    ordering: typing.Tuple[Ordering] = property(operator.itemgetter(2))
    frame: typing.Optional['Window.Frame'] = property(operator.itemgetter(3))

    class Frame(collections.namedtuple('Frame', 'mode, start, end')):
        """Sliding window frame spec.
        """
        @enum.unique
        class Mode(enum.Enum):
            """Frame mode.
            """
            ROWS = 'rows'
            GROUPS = 'groups'
            RANGE = 'range'

    class Function:
        """Window function representation.
        """

        def over(self, partition: typing.Sequence[Element], ordering: typing.Optional[typing.Sequence[typing.Union[
            Element, typing.Union['Ordering.Direction', str], typing.Tuple[Element, typing.Union[
                'Ordering.Direction', str]]]]] = None, frame: typing.Optional = None) -> 'Window':
            """Create a window using this function.

            Args:
                partition: Window partitioning specifying the rows of query results.
                ordering: Order in which input rows should be processed.
                frame: Sliding window specification.

            Returns: Windowed column instance.
            """
            return Window(self, partition, ordering, frame)

    def __new__(cls, function: 'Window.Function', partition: typing.Sequence[Column], ordering: typing.Optional[
        typing.Sequence[typing.Union[Element, typing.Union['Ordering.Direction', str], typing.Tuple[
            Element, typing.Union['Ordering.Direction', str]]]]] = None, frame: typing.Optional = None):
        return super().__new__(cls, function, tuple(partition), Ordering.make(ordering or []), frame)

    @property
    def name(self) -> None:
        """Window has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    @property
    def kind(self) -> kindmod.Any:
        return self.function.kind

    def accept(self, visitor: visit.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_window(self)

    @property
    def element(self) -> 'Element':
        raise NotImplementedError('TODO')


class Aggregate(Arithmetic, Multirow, Window.Function):
    """Base class for column aggregation functions.
    """
