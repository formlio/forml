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
import contextlib
import enum
import functools
import itertools
import logging
import operator
import typing
from collections import abc as colabc

from forml.io.dsl import error
from forml.io.dsl.schema import kind as kindmod, visit as vismod

if typing.TYPE_CHECKING:
    from forml.io.dsl.schema import frame as framod

LOGGER = logging.getLogger(__name__)


def cast(value: typing.Any) -> 'Column':
    """Attempt to create a literal instance of the value unless already a column.

    Args:
        value: Value to be represented as Operable column.

    Returns: Operable column instance.
    """
    if not isinstance(value, Column):
        LOGGER.debug('Converting value of %s to a literal type', value)
        value = Literal(value)
    return value


class Column(tuple, metaclass=abc.ABCMeta):
    """Base class for column types (ie fields or select expressions).
    """
    class Dissect(vismod.Series):
        """Visitor extracting column instances of given type(s).
        """
        def __init__(self, *types: typing.Type['Column']):
            self._types: typing.FrozenSet[typing.Type['Column']] = frozenset(types)
            self._match: typing.Set['Column'] = set()
            self._seen: typing.Set['Column'] = set()

        @property
        def terms(self) -> typing.FrozenSet['Column']:
            """Extracted columns.

            Returns: Set of extracted columns.
            """
            return frozenset(self._match)

        @contextlib.contextmanager
        def visit_source(self, source: 'framod.Source') -> typing.Iterable[None]:
            yield
            for column in source.columns:
                if column in self._seen:
                    continue
                column.accept(self)

        @contextlib.contextmanager
        def visit_column(self, column: 'Column') -> typing.Iterable[None]:
            self._seen.add(column)
            yield
            if any(isinstance(column, t) for t in self._types):
                self._match.add(column)

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(a) for a in self)})'

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
    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """

    @property
    @abc.abstractmethod
    def operable(self) -> 'Operable':
        """Return the operable of this column (apart from Aliased, operable is the column itself).

        Returns: Column's operable.
        """

    @classmethod
    def dissect(cls, *column: 'Column') -> typing.FrozenSet['Column']:
        """Return an iterable of instances of this type composing given column(s).

        Returns: Set of this type instances used in given column(s).
        """
        visitor = cls.Dissect(cls)
        for col in column:
            col.accept(visitor)
        return visitor.terms

    @classmethod
    def ensure_is(cls, column: 'Column') -> 'Column':
        """Ensure given column is of our type.

        Args:
            column: Column to be verified.

        Returns: Original column if instance of our type or raising otherwise.
        """
        column = cast(column)
        if not isinstance(column, cls):
            raise error.Syntax(f'{column} not an instance of a {cls.__name__}')
        return column

    @classmethod
    def ensure_in(cls, column: 'Column') -> 'Column':
        """Ensure given column is composed of our type.

        Args:
            column: Column to be verified.

        Returns: Original column if containing our type or raising otherwise.
        """
        if not cls.dissect(column):
            raise error.Syntax(f'No {cls.__name__} instance(s) found in {column}')
        return column

    @classmethod
    def ensure_notin(cls, column: 'Column') -> 'Column':
        """Ensure given column is not composed of our type.

        Args:
            column: Column to be verified.

        Returns: Original column if not of our type or raising otherwise.
        """
        if cls.dissect(column):
            raise error.Syntax(f'{cls.__name__} instance(s) found in {column}')
        return column


def columnize(handler: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
    """Decorator for forcing function arguments to operable columns.

    Args:
        handler: Callable to be decorated.

    Returns: Decorated callable.
    """
    @functools.wraps(handler)
    def wrapper(*args: typing.Any) -> typing.Sequence['Operable']:
        """Actual decorator.

        Args:
            *args: Arguments to be forced to columns.

        Returns: Arguments converted to columns.
        """
        return handler(*(cast(a).operable for a in args))

    return wrapper


class Operable(Column, metaclass=abc.ABCMeta):
    """Base class for columns that can be used in expressions, conditions, grouping and/or ordering definitions.
    """
    @property
    def operable(self) -> 'Operable':
        return self

    @classmethod
    def ensure_is(cls, column: 'Column') -> 'Operable':
        """Ensure given given column is an Operable.
        """
        return super().ensure_is(column).operable

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias:

        Returns: New column instance with given alias.
        """
        return Aliased(self, alias)

    __hash__ = Column.__hash__  # otherwise gets overwritten to None due to redefined __eq__

    @columnize
    def __eq__(self, other: 'Operable') -> 'Equal':
        return Equal(self, other)

    @columnize
    def __ne__(self, other: 'Operable') -> 'NotEqual':
        return NotEqual(self, other)

    @columnize
    def __lt__(self, other: 'Operable') -> 'LessThan':
        return LessThan(self, other)

    @columnize
    def __le__(self, other: 'Operable') -> 'LessEqual':
        return LessEqual(self, other)

    @columnize
    def __gt__(self, other: 'Operable') -> 'GreaterThan':
        return GreaterThan(self, other)

    @columnize
    def __ge__(self, other: 'Operable') -> 'GreaterEqual':
        return GreaterEqual(self, other)

    @columnize
    def __and__(self, other: 'Operable') -> 'And':
        return And(self, other)

    @columnize
    def __rand__(self, other: 'Operable') -> 'And':
        return And(other, self)

    @columnize
    def __or__(self, other: 'Operable') -> 'Or':
        return Or(self, other)

    @columnize
    def __ror__(self, other: 'Operable') -> 'Or':
        return Or(other, self)

    def __invert__(self) -> 'Not':
        return Not(self)

    @columnize
    def __add__(self, other: 'Operable') -> 'Addition':
        return Addition(self, other)

    @columnize
    def __radd__(self, other: 'Operable') -> 'Addition':
        return Addition(other, self)

    @columnize
    def __sub__(self, other: 'Operable') -> 'Subtraction':
        return Subtraction(self, other)

    @columnize
    def __rsub__(self, other: 'Operable') -> 'Subtraction':
        return Subtraction(other, self)

    @columnize
    def __mul__(self, other: 'Operable') -> 'Multiplication':
        return Multiplication(self, other)

    @columnize
    def __rmul__(self, other: 'Operable') -> 'Multiplication':
        return Multiplication(other, self)

    @columnize
    def __truediv__(self, other: 'Operable') -> 'Division':
        return Division(self, other)

    @columnize
    def __rtruediv__(self, other: 'Operable') -> 'Division':
        return Division(other, self)

    @columnize
    def __mod__(self, other: 'Operable') -> 'Modulus':
        return Modulus(self, other)

    @columnize
    def __rmod__(self, other: 'Operable') -> 'Modulus':
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

        @classmethod
        def _missing_(cls, value):
            if isinstance(value, str):
                value = value.lower()
                if value in {'asc', 'ascending'}:
                    return cls.ASCENDING
                if value in {'desc', 'descending'}:
                    return cls.DESCENDING
            return super()._missing_(value)

        def __repr__(self):
            return f'<{self.value}>'

        def __call__(self, column: typing.Union[Operable, 'Ordering']) -> 'Ordering':
            if isinstance(column, Ordering):
                column = column.column
            return Ordering(column, self)

    def __new__(cls, column: Operable,
                direction: typing.Optional[typing.Union['Ordering.Direction', str]] = None):
        return super().__new__(cls, Operable.ensure_is(column),
                               cls.Direction(direction) if direction else cls.Direction.ASCENDING)

    def __repr__(self):
        return f'{repr(self.column)}{repr(self.direction)}'

    @classmethod
    def make(cls, specs: typing.Sequence[typing.Union[Operable, typing.Union[
        'Ordering.Direction', str], typing.Tuple[Operable, typing.Union[
            'Ordering.Direction', str]]]]) -> typing.Iterable['Ordering']:
        """Helper to generate orderings from given columns and directions.

        Args:
            specs: One or many columns or actual ordering instances.

        Returns: Sequence of ordering terms.
        """
        specs = itertools.zip_longest(specs, specs[1:])
        for column, direction in specs:
            if isinstance(column, Column):
                if isinstance(direction, (Ordering.Direction, str)):
                    yield Ordering.Direction(direction)(column)
                    next(specs)  # pylint: disable=stop-iteration-return
                else:
                    yield Ordering(column)
            elif isinstance(column, colabc.Sequence) and len(column) == 2:
                column, direction = column
                yield Ordering.Direction(direction)(column)
            else:
                raise error.Syntax('Expecting pair of column and direction')


class Aliased(Column):
    """Aliased column representation.
    """
    operable: Operable = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, column: Column, alias: str):
        return super().__new__(cls, [column.operable, alias])

    def __repr__(self):
        return f'{self.name}=[{repr(self.operable)}]'

    @property
    def kind(self) -> kindmod.Any:
        """Column type.

        Returns: Inner column type.
        """
        return self.operable.kind

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        with visitor.visit_aliased(self):
            self.operable.accept(visitor)


class Literal(Operable):
    """Literal value.
    """
    value: typing.Any = property(operator.itemgetter(0))
    kind: kindmod.Any = property(operator.itemgetter(1))

    def __new__(cls, value: typing.Any):
        return super().__new__(cls, [value, kindmod.reflect(value)])

    def __repr__(self):
        return repr(self.value)

    @property
    def name(self) -> None:
        """Literal has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        with visitor.visit_literal(self):
            pass


class Element(Operable):
    """Named column of particular source.
    """
    source: 'framod.Source' = property(operator.itemgetter(0))
    name: str = property(operator.itemgetter(1))

    def __new__(cls, table: 'framod.Tangible', name: str):
        return super().__new__(cls, [table, name])

    def __repr__(self):
        return f'{repr(self.source)}.{self.name}'

    @property
    def kind(self) -> kindmod.Any:
        return self.source.schema[self.name].kind

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        with visitor.visit_element(self):
            self.source.accept(visitor)


class Field(Element):
    """Special type of element is the schema field type.
    """
    source: 'framod.Table' = property(operator.itemgetter(0))


class Expression(Operable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for expressions.
    """
    def __new__(cls, *terms: Operable):
        return super().__new__(cls, terms)

    @property
    def name(self) -> None:
        """Expression has no name without an explicit aliasing.

        Returns: None.
        """
        return None

    def accept(self, visitor: vismod.Series) -> None:
        with visitor.visit_expression(self):
            for term in self:
                if isinstance(term, Operable):
                    term.accept(visitor)


class Univariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of just one argument/operand.
    """
    def __new__(cls, arg: Operable):
        return super().__new__(cls, arg)


class Bivariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands.
    """
    def __new__(cls, arg1: Operable, arg2: Operable):
        return super().__new__(cls, arg1, arg2)


class Operator(metaclass=abc.ABCMeta):
    """Mixin for an operator expression.
    """

    @property
    @abc.abstractmethod
    def symbol(self) -> str:
        """Operator symbol.

        Returns: String representation of the symbol.
        """


class Infix(Operator, Bivariate, metaclass=abc.ABCMeta):
    """Base class for infix operator expressions.
    """
    def __repr__(self):
        return f'{repr(self[0])} {self.symbol} {repr(self[1])}'


class Postfix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for postfix operator expressions.
    """
    def __repr__(self):
        return f'{repr(self[0])} {self.symbol}'


class Prefix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for prefix operator expressions.
    """
    def __repr__(self):
        return f'{self.symbol} {repr(self[0])}'


class Logical:
    """Mixin for logical functions/operators.
    """
    kind = kindmod.Boolean()

    @classmethod
    def ensure_is(cls, column: Operable) -> Operable:
        """Ensure given expression is a logical one.
        """
        if not isinstance(column.kind, cls.kind.__class__):
            raise error.Syntax(f'{column.kind} not a valid {cls.kind}')
        return column

    @property
    def predicates(self) -> typing.FrozenSet['Logical']:
        """Get subset of primitive predicates - logical expressions involving just a single Field.

        Returns: Set of predicates involved in this expression.
        """
        return {c for c in self.dissect(self) if len(Field.dissect(c)) == 1}  # pylint: disable=no-member


class LessThan(Logical, Infix):
    """Less-Than operator.
    """
    symbol = '<'


class LessEqual(Logical, Infix):
    """Less-Equal operator.
    """
    symbol = '<='


class GreaterThan(Logical, Infix):
    """Greater-Than operator.
    """
    symbol = '>'


class GreaterEqual(Logical, Infix):
    """Greater-Equal operator.
    """
    symbol = '>='


class Equal(Logical, Infix):
    """Equal operator.
    """
    symbol = '=='

    def __bool__(self):
        """Since this instance is also returned when python internally compares two Column instances for equality, we
        want to evaluate the boolean value for python perspective of the objects (rather than just the ETL perspective
        of the data).

        Note this doesn't reflect mathematical commutativity - order of potential sub-expression operands matters.
        """
        return hash(self[0]) == hash(self[1])


class NotEqual(Logical, Infix):
    """Not-Equal operator.
    """
    symbol = '!='


class IsNull(Logical, Postfix):
    """Is-Null operator.
    """
    symbol = 'IS NULL'


class NotNull(Logical, Postfix):
    """Not-Null operator.
    """
    symbol = 'NOT NULL'


class And(Logical, Infix):
    """And operator.
    """
    symbol = 'AND'


class Or(Logical, Infix):
    """Or operator.
    """
    symbol = 'OR'


class Not(Logical, Prefix):
    """Not operator.
    """
    symbol = 'NOT'


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


class Addition(Arithmetic, Infix):
    """Plus operator.
    """
    symbol = '+'


class Subtraction(Arithmetic, Infix):
    """Minus operator.
    """
    symbol = '-'


class Multiplication(Arithmetic, Infix):
    """Times operator.
    """
    symbol = '*'


class Division(Arithmetic, Infix):
    """Divide operator.
    """
    symbol = '/'


class Modulus(Arithmetic, Infix):
    """Modulus operator.
    """
    symbol = '%'


class Multirow(Expression, metaclass=abc.ABCMeta):
    """Base class for expressions involving cross-row operations.
    """


class Window(Multirow):
    """Window type column representation.
    """
    function: 'Window.Function' = property(operator.itemgetter(0))
    partition: typing.Tuple[Operable] = property(operator.itemgetter(1))
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

        def over(self, partition: typing.Sequence[Operable], ordering: typing.Optional[typing.Sequence[typing.Union[
            Operable, typing.Union['Ordering.Direction', str], typing.Tuple[Operable, typing.Union[
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
        typing.Sequence[typing.Union[Operable, typing.Union['Ordering.Direction', str], typing.Tuple[
            Operable, typing.Union['Ordering.Direction', str]]]]] = None, frame: typing.Optional = None):
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

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        with visitor.visit_window(self):
            pass


class Aggregate(Arithmetic, Multirow, Window.Function):
    """Base class for column aggregation functions.
    """
