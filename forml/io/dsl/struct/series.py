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
import operator as opermod
import types
import typing
from collections import abc as colabc

from forml.io.dsl import error
from forml.io.dsl.struct import frame as framod, kind as kindmod, visit as vismod

LOGGER = logging.getLogger(__name__)


def cast(value: typing.Any) -> 'Column':
    """Attempt to create a literal instance of the value unless already a column.

    Args:
        value: Value to be represented as Operable column.

    Returns:
        Operable column instance.
    """
    if not isinstance(value, Column):
        LOGGER.debug('Converting value of %s to a literal type', value)
        value = Literal(value)
    return value


class Column(tuple, metaclass=abc.ABCMeta):
    """Base class for column types (ie fields or select expressions)."""

    class Dissect(vismod.Series):
        """Visitor extracting column instances of given type(s)."""

        def __init__(self, *types: typing.Type):
            self._types: typing.FrozenSet[typing.Type] = frozenset(types)
            self._match: typing.Set['Column'] = set()
            self._seen: typing.Set['Column'] = set()

        def __call__(self, *column: 'Column') -> typing.FrozenSet['Column']:
            """Apply this dissector to the given columns.

            Returns:
                Set of instances matching the registered types used in given column(s).
            """
            for col in column:
                col.accept(self)
            return frozenset(self._match)

        def visit_origin(self, source: 'framod.Source') -> None:
            for column in source.columns:
                if column not in self._seen:
                    self._seen.add(column)
                    column.accept(self)

        def visit_column(self, column: 'Column') -> None:
            if any(isinstance(column, t) for t in self._types):
                self._match.add(column)

    def __new__(cls, *args):
        return super().__new__(cls, args)

    def __getnewargs__(self):
        return tuple(self)

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join(repr(a) for a in self)})'

    def __hash__(self):
        return hash(self.__class__) ^ super().__hash__()

    @property
    @abc.abstractmethod
    def name(self) -> typing.Optional[str]:
        """Column nme

        Returns:
            name string.
        """

    @property
    @abc.abstractmethod
    def kind(self) -> kindmod.Any:
        """Column type.

        Returns:
            type.
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

        Returns:
            Column's operable.
        """

    @classmethod
    def dissect(cls, *column: 'Column') -> typing.FrozenSet['Column']:
        """Return an iterable of instances of this type composing given column(s).

        Returns:
            Set of this type instances used in given column(s).
        """
        return cls.Dissect(cls)(*column)

    @classmethod
    def ensure_is(cls, column: 'Column') -> 'Column':
        """Ensure given column is of our type.

        Args:
            column: Column to be verified.

        Returns:
            Original column if instance of our type or raising otherwise.
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

        Returns:
            Original column if containing our type or raising otherwise.
        """
        if not cls.dissect(column):
            raise error.Syntax(f'No {cls.__name__} instance(s) found in {column}')
        return column

    @classmethod
    def ensure_notin(cls, column: 'Column') -> 'Column':
        """Ensure given column is not composed of our type.

        Args:
            column: Column to be verified.

        Returns:
            Original column if not of our type or raising otherwise.
        """
        if cls.dissect(column):
            raise error.Syntax(f'{cls.__name__} instance(s) found in {column}')
        return column


def columnize(handler: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
    """Decorator for forcing function arguments to operable columns.

    Args:
        handler: Callable to be decorated.

    Returns:
        Decorated callable.
    """

    @functools.wraps(handler)
    def wrapper(*args: typing.Any) -> typing.Sequence['Operable']:
        """Actual decorator.

        Args:
            *args: Arguments to be forced to columns.

        Returns:
            Arguments converted to columns.
        """
        return handler(*(cast(a).operable for a in args))

    return wrapper


class Operable(Column, metaclass=abc.ABCMeta):
    """Base class for columns that can be used in expressions, conditions, grouping and/or ordering definitions."""

    @property
    def operable(self) -> 'Operable':
        return self

    @classmethod
    def ensure_is(cls, column: 'Column') -> 'Operable':
        """Ensure given given column is an Operable."""
        return super().ensure_is(column).operable

    def alias(self, alias: str) -> 'Aliased':
        """Use an alias for this column.

        Args:
            alias: Aliased column name.

        Returns:
            New column instance with given alias.
        """
        return Aliased(self, alias)

    __hash__ = Column.__hash__  # otherwise gets overwritten to None due to redefined __eq__

    @columnize
    def __eq__(self, other: 'Operable') -> 'Equal':
        return Comparison.Pythonic(Equal, self, other)

    @columnize
    def __ne__(self, other: 'Operable') -> 'NotEqual':
        return NotEqual(self, other)

    @columnize
    def __lt__(self, other: 'Operable') -> 'LessThan':
        return Comparison.Pythonic(LessThan, self, other)

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
    """OrderBy spec."""

    @enum.unique
    class Direction(enum.Enum):
        """Ordering direction."""

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

    def __new__(cls, column: Operable, direction: typing.Optional[typing.Union['Ordering.Direction', str]] = None):
        return super().__new__(
            cls, Operable.ensure_is(column), cls.Direction(direction) if direction else cls.Direction.ASCENDING
        )

    def __repr__(self):
        return f'{repr(self.column)}{repr(self.direction)}'

    @classmethod
    def make(
        cls,
        specs: typing.Sequence[
            typing.Union[
                Operable,
                typing.Union['Ordering.Direction', str],
                typing.Tuple[Operable, typing.Union['Ordering.Direction', str]],
            ]
        ],
    ) -> typing.Iterable['Ordering']:
        """Helper to generate orderings from given columns and directions.

        Args:
            specs: One or many columns or actual ordering instances.

        Returns:
            Sequence of ordering terms.
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
    """Aliased column representation."""

    operable: Operable = property(opermod.itemgetter(0))
    name: str = property(opermod.itemgetter(1))

    def __new__(cls, column: Column, alias: str):
        return super().__new__(cls, column.operable, alias)

    def __repr__(self):
        return f'{self.name}=[{repr(self.operable)}]'

    @property
    def kind(self) -> kindmod.Any:
        """Column type.

        Returns:
            Inner column type.
        """
        return self.operable.kind

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_aliased(self)


class Literal(Operable):
    """Literal value."""

    value: typing.Any = property(opermod.itemgetter(0))
    kind: kindmod.Any = property(opermod.itemgetter(1))

    def __new__(cls, value: typing.Any):
        return super().__new__(cls, value, kindmod.reflect(value))

    def __getnewargs__(self):
        return tuple([self.value])

    def __repr__(self):
        return repr(self.value)

    @property
    def name(self) -> None:
        """Literal has no name without an explicit aliasing.

        Returns:
            None.
        """
        return None

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_literal(self)


class Element(Operable):
    """Named column of particular source."""

    origin: 'framod.Origin' = property(opermod.itemgetter(0))
    name: str = property(opermod.itemgetter(1))

    def __new__(cls, source: 'framod.Origin', name: str):
        if isinstance(source, framod.Table) and not issubclass(cls, Field):
            return Field(source, name)
        return super().__new__(cls, source, name)

    def __repr__(self):
        return f'{repr(self.origin)}.{self.name}'

    @property
    def kind(self) -> kindmod.Any:
        return self.origin.schema[self.name].kind

    def accept(self, visitor: vismod.Series) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_element(self)


class Field(Element):
    """Special type of element is the schema field type."""

    origin: 'framod.Table' = property(opermod.itemgetter(0))

    def __new__(cls, table: 'framod.Table', name: str):
        if not isinstance(table, framod.Table):
            raise ValueError('Invalid field source')
        return super().__new__(cls, table, name)


class Expression(Operable, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for expressions."""

    @property
    def name(self) -> None:
        """Expression has no name without an explicit aliasing.

        Returns:
            None.
        """
        return None

    def accept(self, visitor: vismod.Series) -> None:
        visitor.visit_expression(self)


class Univariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of just one argument/operand."""

    def __new__(cls, arg: Operable):
        return super().__new__(cls, Operable.ensure_is(arg))


class Bivariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands."""

    def __new__(cls, arg1: Operable, arg2: Operable):
        return super().__new__(cls, Operable.ensure_is(arg1), Operable.ensure_is(arg2))


class Operator(metaclass=abc.ABCMeta):
    """Mixin for an operator expression."""

    @property
    @abc.abstractmethod
    def symbol(self) -> str:
        """Operator symbol.

        Returns:
            String representation of the symbol.
        """


class Infix(Operator, Bivariate, metaclass=abc.ABCMeta):
    """Base class for infix operator expressions."""

    left: Operable = property(opermod.itemgetter(0))
    right: Operable = property(opermod.itemgetter(1))

    def __repr__(self):
        return f'{repr(self[0])} {self.symbol} {repr(self[1])}'


class Prefix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for prefix operator expressions."""

    operand: Operable = property(opermod.itemgetter(0))

    def __repr__(self):
        return f'{self.symbol} {repr(self[0])}'


class Postfix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for postfix operator expressions."""

    operand: Operable = property(opermod.itemgetter(0))

    def __new__(cls, arg: Operable):
        return super().__new__(cls, Operable.ensure_is(arg))

    def __repr__(self):
        return f'{repr(self[0])} {self.symbol}'


class Predicate(metaclass=abc.ABCMeta):
    """Base class for Logical and Comparison operators."""

    class Factors(typing.Mapping['framod.Table', 'Factors']):
        """Mapping (read-only) of predicate factors to their tables. Factor is pa predicate which is involving exactly
        one and only table.
        """

        def __init__(self, *predicates: 'Predicate'):
            items = {p: {f.origin for f in Field.dissect(p)} for p in predicates}
            if collections.Counter(len(s) == 1 for s in items.values())[True] != len(predicates):
                raise ValueError('Repeated or non-primitive predicates')
            self._items: typing.Mapping[framod.Table, Predicate] = types.MappingProxyType(
                {s.pop(): p for p, s in items.items()}
            )

        @classmethod
        def merge(
            cls,
            left: 'Predicate.Factors',
            right: 'Predicate.Factors',
            operator: typing.Callable[['Predicate', 'Predicate'], 'Predicate'],
        ) -> 'Predicate.Factors':
            """Merge the two primitive predicates.

            Args:
                left: Left primitive to be merged.
                right: Right primitive to be merged.
                operator: Operator to be used for combining individual predicates.

            Returns:
                New Primitive instance with individual predicates combined.
            """
            return cls(
                *(
                    operator(left[k], right[k])
                    if k in left and k in right and hash(left[k]) != hash(right[k])
                    else left[k]
                    if k in left
                    else right
                    for k in left.keys() | right.keys()
                )
            )

        def __and__(self, other: 'Predicate.Factors') -> 'Predicate.Factors':
            return self.merge(self, other, And)

        def __or__(self, other: 'Predicate.Factors') -> 'Predicate.Factors':
            return self.merge(self, other, Or)

        def __getitem__(self, table: 'framod.Table') -> 'Predicate':
            return self._items[table]

        def __len__(self) -> int:
            return len(self._items)

        def __iter__(self) -> typing.Iterator['framod.Table']:
            return iter(self._items)

    kind = kindmod.Boolean()

    @property
    @abc.abstractmethod
    def factors(self) -> 'Predicate.Factors':
        """Mapping of primitive source predicates - involving just a single Table.

        Returns:
            Break down of factors involved in this predicate.
        """

    @classmethod
    def ensure_is(cls: typing.Type[Operable], column: Operable) -> Operable:
        """Ensure given column is a predicate. Since this mixin class is supposed to be used as a first base class of
        its column implementors, this will mask the Column.ensure_is API. Here we add special implementation depending
        on whether it is used directly on the Predicate class or its bare mixin subclasses or the actual Column
        implementation using this mixin.

        Args:
            column: Column instance to be checked for its compliance.

        Returns:
            Column instance.
        """
        column = Operable.ensure_is(column)
        if cls is Predicate:  # bare Predicate - accept anything of a boolean kind.
            kindmod.Boolean.ensure(column.kind)
        elif not issubclass(cls, Column):  # bare Predicate mixin subclasses
            if not isinstance(column, cls):
                raise error.Syntax(f'{column} not an instance of a {cls.__name__}')
        else:  # defer to the column's .ensure_is implementation
            column = next(b for b in cls.__bases__ if issubclass(b, Column)).ensure_is(column)
        return column


class Logical(Predicate, metaclass=abc.ABCMeta):
    """Mixin for logical operators."""

    def __init__(self, *operands: Operable):
        for arg in operands:
            Predicate.ensure_is(arg)


class And(Logical, Infix):
    """And operator."""

    symbol = 'AND'

    @property
    @functools.lru_cache()
    def factors(self: 'And') -> 'Predicate.Factors':
        return self.left.factors & self.right.factors


class Or(Logical, Infix):
    """Or operator."""

    symbol = 'OR'

    @property
    @functools.lru_cache()
    def factors(self: 'Or') -> 'Predicate.Factors':
        return self.left.factors | self.right.factors


class Not(Logical, Prefix):
    """Not operator."""

    symbol = 'NOT'

    @property
    def factors(self: 'Not') -> 'Predicate.Factors':
        return self.operand.factors


class Comparison(Predicate):
    """Mixin for comparison operators."""

    class Pythonic(Operable):
        """Semi proxy/lazy wrapper allowing native Python features like sorting or equality tests to work transparently
        without raising the syntax errors implemented in the constructors of the actual Comparison types.

        This instance is expected to be used only internally by Python itself. All code within ForML is supposed to use
        the extracted .operable instance of the true Comparison type.
        """

        operator: typing.Type[Infix] = property(opermod.itemgetter(0))
        left: Operable = property(opermod.itemgetter(1))
        right: Operable = property(opermod.itemgetter(2))

        def __new__(cls, operator: typing.Type[Infix], left: Operable, right: Operable):
            return super().__new__(cls, operator, left, right)

        def __bool__(self):
            if self.operator is Equal:
                return hash(self.left) == hash(self.right)
            if self.operator is LessThan:
                return repr(self.left) < repr(self.right)
            raise RuntimeError(f'Unexpected Pythonic comparison using {self.operator}')

        @property
        def name(self) -> typing.Optional[str]:
            raise RuntimeError('Pythonic comparison proxy used as a column')

        @property
        def kind(self) -> kindmod.Any:
            raise RuntimeError('Pythonic comparison proxy used as a column')

        def accept(self, visitor: vismod.Series) -> None:
            raise RuntimeError('Pythonic comparison proxy used as a column')

        @property
        def operable(self) -> Infix:
            """Materialize the real Comparison instance represented by this proxy.

            Returns:
                Comparison instance.
            """
            return self.operator(self.left, self.right)

    def __init__(self, *operands: Operable):
        operands = [Operable.ensure_is(o) for o in operands]
        if not (
            all(kindmod.Numeric.match(o.kind) for o in operands) or all(o.kind == operands[0].kind for o in operands)
        ):
            raise error.Syntax(f'Invalid operands for {self} comparison')

    @property
    @functools.lru_cache()
    def factors(self: 'Comparison') -> Predicate.Factors:
        return Predicate.Factors(self) if len({f.origin for f in Field.dissect(self)}) == 1 else Predicate.Factors()


class LessThan(Comparison, Infix):
    """Less-Than operator."""

    symbol = '<'


class LessEqual(Comparison, Infix):
    """Less-Equal operator."""

    symbol = '<='


class GreaterThan(Comparison, Infix):
    """Greater-Than operator."""

    symbol = '>'


class GreaterEqual(Comparison, Infix):
    """Greater-Equal operator."""

    symbol = '>='


class Equal(Comparison, Infix):
    """Equal operator."""

    symbol = '=='

    def __bool__(self):
        """Since this instance is also returned when python internally compares two Column instances for equality (ie
        when the instance is stored within a hash-based container), we want to evaluate the boolean value for python
        perspective of the objects (rather than just the ETL perspective of the data).

        Note this doesn't reflect mathematical commutativity - order of potential sub-expression operands matters.
        """
        return hash(self.left) == hash(self.right)


class NotEqual(Comparison, Infix):
    """Not-Equal operator."""

    symbol = '!='


class IsNull(Comparison, Postfix):
    """Is-Null operator."""

    symbol = 'IS NULL'


class NotNull(Comparison, Postfix):
    """Not-Null operator."""

    symbol = 'NOT NULL'


class Arithmetic:
    """Mixin for numerical operators."""

    def __init__(self, *operands: Operable):
        operands = [Operable.ensure_is(o) for o in operands]
        if not all(kindmod.Numeric.match(o.kind) for o in operands):
            raise error.Syntax(f'Invalid arithmetic operands for {self}')

    @property
    def kind(self) -> kindmod.Numeric:
        """Largest cardinality kind of all operators kinds.

        Returns:
            Numeric kind.
        """
        return functools.reduce(
            functools.partial(max, key=lambda k: k.__cardinality__),
            (o.kind for o in self),  # pylint: disable=not-an-iterable
        )


class Addition(Arithmetic, Infix):
    """Plus operator."""

    symbol = '+'


class Subtraction(Arithmetic, Infix):
    """Minus operator."""

    symbol = '-'


class Multiplication(Arithmetic, Infix):
    """Times operator."""

    symbol = '*'


class Division(Arithmetic, Infix):
    """Divide operator."""

    symbol = '/'


class Modulus(Arithmetic, Infix):
    """Modulus operator."""

    symbol = '%'


class Cumulative(Expression, metaclass=abc.ABCMeta):
    """Base class for expressions involving cross-row operations."""


class Window(Cumulative):
    """Window type column representation."""

    function: 'Window.Function' = property(opermod.itemgetter(0))
    partition: typing.Tuple[Operable] = property(opermod.itemgetter(1))
    ordering: typing.Tuple[Ordering] = property(opermod.itemgetter(2))
    frame: typing.Optional['Window.Frame'] = property(opermod.itemgetter(3))

    class Frame(collections.namedtuple('Frame', 'mode, start, end')):
        """Sliding window frame spec."""

        @enum.unique
        class Mode(enum.Enum):
            """Frame mode."""

            ROWS = 'rows'
            GROUPS = 'groups'
            RANGE = 'range'

    class Function:
        """Window function representation."""

        def over(
            self,
            partition: typing.Sequence[Operable],
            ordering: typing.Optional[
                typing.Sequence[
                    typing.Union[
                        Operable,
                        typing.Union['Ordering.Direction', str],
                        typing.Tuple[Operable, typing.Union['Ordering.Direction', str]],
                    ]
                ]
            ] = None,
            frame: typing.Optional = None,
        ) -> 'Window':
            """Create a window using this function.

            Args:
                partition: Window partitioning specifying the rows of query results.
                ordering: Order in which input rows should be processed.
                frame: Sliding window specification.

            Returns:
                Windowed column instance.
            """
            return Window(self, partition, ordering, frame)

    def __new__(
        cls,
        function: 'Window.Function',
        partition: typing.Sequence[Column],
        ordering: typing.Optional[
            typing.Sequence[
                typing.Union[
                    Operable,
                    typing.Union['Ordering.Direction', str],
                    typing.Tuple[Operable, typing.Union['Ordering.Direction', str]],
                ]
            ]
        ] = None,
        frame: typing.Optional = None,
    ):
        return super().__new__(cls, function, tuple(partition), Ordering.make(ordering or []), frame)

    @property
    def name(self) -> None:
        """Window has no name without an explicit aliasing.

        Returns:
            None.
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
        visitor.visit_window(self)


class Aggregate(Cumulative, Window.Function, metaclass=abc.ABCMeta):
    """Base class for column aggregation functions."""
