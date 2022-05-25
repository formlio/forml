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

from .. import _exception
from . import frame as framod
from . import kind as kindmod

if typing.TYPE_CHECKING:
    from forml.io import dsl


LOGGER = logging.getLogger(__name__)


def cast(value: typing.Any) -> 'dsl.Feature':
    """Attempt to create a literal instance of the value unless already a feature.

    Args:
        value: Value to be represented as Operable feature.

    Returns:
        Operable feature instance.
    """
    if not isinstance(value, Feature):
        LOGGER.debug('Converting value of %s to a literal type', value)
        value = Literal(value)
    return value


class Feature(tuple, metaclass=abc.ABCMeta):
    """Base class for feature types (ie fields or select expressions)."""

    class Visitor:
        """Feature visitor."""

        def visit_feature(self, feature: 'dsl.Feature') -> None:  # pylint: disable=unused-argument
            """Generic feature hook.

            Args:
                feature: 'dsl.Feature' instance to be visited.
            """

        def visit_aliased(self, feature: 'dsl.Aliased') -> None:
            """Generic expression hook.

            Args:
                feature: Aliased feature instance to be visited.
            """
            feature.operable.accept(self)
            self.visit_feature(feature)

        def visit_element(self, feature: 'dsl.Element') -> None:
            """Generic expression hook.

            Args:
                feature: Element instance to be visited.
            """
            self.visit_feature(feature)

        def visit_literal(self, feature: 'dsl.Literal') -> None:
            """Generic literal hook.

            Args:
                feature: Literal instance to be visited.
            """
            self.visit_feature(feature)

        def visit_expression(self, feature: 'dsl.Expression') -> None:
            """Generic expression hook.

            Args:
                feature: Expression instance to be visited.
            """
            for term in feature:
                if isinstance(term, Feature):
                    term.accept(self)
            self.visit_feature(feature)

        def visit_window(self, feature: 'dsl.Window') -> None:
            """Generic window hook.

            Args:
                feature: Window instance to be visited.
            """
            self.visit_feature(feature)

    class Dissect(Visitor):
        """Visitor extracting feature instances of given type(s)."""

        def __init__(self, *types: type):
            self._types: frozenset[type] = frozenset(types)
            self._match: set['Feature'] = set()

        def __call__(self, *feature: 'dsl.Feature') -> frozenset['dsl.Feature']:
            """Apply this dissector to the given features.

            Args:
                feature: Sequence of features to dissect.

            Returns:
                Set of instances matching the registered types used in given feature(s).
            """
            for col in feature:
                col.accept(self)
            return frozenset(self._match)

        def visit_feature(self, feature: 'dsl.Feature') -> None:
            if any(isinstance(feature, t) for t in self._types):
                self._match.add(feature)

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
        """Feature name.

        Returns:
            Name string.
        """

    @property
    @abc.abstractmethod
    def kind(self) -> 'dsl.Any':
        """Feature type.

        Returns:
            Type.
        """

    @abc.abstractmethod
    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """

    @property
    @abc.abstractmethod
    def operable(self) -> 'dsl.Operable':
        """Return the operable of this feature (apart from Aliased, operable is the feature itself).

        Returns:
            Feature's operable.
        """

    @classmethod
    def dissect(cls, *feature: 'dsl.Feature') -> frozenset['dsl.Feature']:
        """Return an iterable of instances of this type composing given feature(s).

        Args:
            feature: Sequence of features to dissect.

        Returns:
            Set of this type instances used in given feature(s).
        """
        return cls.Dissect(cls)(*feature)

    @classmethod
    def ensure_is(cls, feature: 'dsl.Feature') -> 'dsl.Feature':
        """Ensure given feature is of our type.

        Args:
            feature: 'dsl.Feature' to be verified.

        Returns:
            Original feature if instance of our type or raising otherwise.
        """
        feature = cast(feature)
        if not isinstance(feature, cls):
            raise _exception.GrammarError(f'{feature} not an instance of a {cls.__name__}')
        return feature

    @classmethod
    def ensure_in(cls, feature: 'dsl.Feature') -> 'dsl.Feature':
        """Ensure given feature is composed of our type.

        Args:
            feature: 'dsl.Feature' to be verified.

        Returns:
            Original feature if containing our type or raising otherwise.
        """
        if not cls.dissect(feature):
            raise _exception.GrammarError(f'No {cls.__name__} instance(s) found in {feature}')
        return feature

    @classmethod
    def ensure_notin(cls, feature: 'dsl.Feature') -> 'dsl.Feature':
        """Ensure given feature is not composed of our type.

        Args:
            feature: 'dsl.Feature' to be verified.

        Returns:
            Original feature if not of our type or raising otherwise.
        """
        if cls.dissect(feature):
            raise _exception.GrammarError(f'{cls.__name__} instance(s) found in {feature}')
        return feature


def featurize(handler: typing.Callable[..., typing.Any]) -> typing.Callable[..., typing.Any]:
    """Decorator for forcing function arguments to operable features.

    Args:
        handler: Callable to be decorated.

    Returns:
        Decorated callable.
    """

    @functools.wraps(handler)
    def wrapper(*args: typing.Any) -> typing.Sequence['dsl.Operable']:
        """Actual decorator.

        Args:
            *args: Arguments to be forced to features.

        Returns:
            Arguments converted to features.
        """
        return handler(*(cast(a).operable for a in args))

    return wrapper


class Operable(Feature, metaclass=abc.ABCMeta):
    """Base class for features that can be used in expressions, conditions, grouping and/or ordering definitions."""

    @property
    def operable(self) -> 'dsl.Operable':
        return self

    @classmethod
    def ensure_is(cls, feature: 'dsl.Feature') -> 'dsl.Operable':
        """Ensure given given feature is an Operable."""
        return super().ensure_is(feature).operable

    def alias(self, alias: str) -> 'dsl.Aliased':
        """Use an alias for this feature.

        Args:
            alias: Aliased feature name.

        Returns:
            New feature instance with given alias.
        """
        return Aliased(self, alias)

    __hash__ = Feature.__hash__  # otherwise gets overwritten to None due to redefined __eq__

    @featurize
    def __eq__(self, other: 'dsl.Operable') -> 'Equal':
        return Comparison.Pythonic(Equal, self, other)

    @featurize
    def __ne__(self, other: 'dsl.Operable') -> 'NotEqual':
        return NotEqual(self, other)

    @featurize
    def __lt__(self, other: 'dsl.Operable') -> 'LessThan':
        return Comparison.Pythonic(LessThan, self, other)

    @featurize
    def __le__(self, other: 'dsl.Operable') -> 'LessEqual':
        return LessEqual(self, other)

    @featurize
    def __gt__(self, other: 'dsl.Operable') -> 'GreaterThan':
        return GreaterThan(self, other)

    @featurize
    def __ge__(self, other: 'dsl.Operable') -> 'GreaterEqual':
        return GreaterEqual(self, other)

    @featurize
    def __and__(self, other: 'dsl.Operable') -> 'And':
        return And(self, other)

    @featurize
    def __rand__(self, other: 'dsl.Operable') -> 'And':
        return And(other, self)

    @featurize
    def __or__(self, other: 'dsl.Operable') -> 'Or':
        return Or(self, other)

    @featurize
    def __ror__(self, other: 'dsl.Operable') -> 'Or':
        return Or(other, self)

    def __invert__(self) -> 'Not':
        return Not(self)

    @featurize
    def __add__(self, other: 'dsl.Operable') -> 'Addition':
        return Addition(self, other)

    @featurize
    def __radd__(self, other: 'dsl.Operable') -> 'Addition':
        return Addition(other, self)

    @featurize
    def __sub__(self, other: 'dsl.Operable') -> 'Subtraction':
        return Subtraction(self, other)

    @featurize
    def __rsub__(self, other: 'dsl.Operable') -> 'Subtraction':
        return Subtraction(other, self)

    @featurize
    def __mul__(self, other: 'dsl.Operable') -> 'Multiplication':
        return Multiplication(self, other)

    @featurize
    def __rmul__(self, other: 'dsl.Operable') -> 'Multiplication':
        return Multiplication(other, self)

    @featurize
    def __truediv__(self, other: 'dsl.Operable') -> 'Division':
        return Division(self, other)

    @featurize
    def __rtruediv__(self, other: 'dsl.Operable') -> 'Division':
        return Division(other, self)

    @featurize
    def __mod__(self, other: 'dsl.Operable') -> 'Modulus':
        return Modulus(self, other)

    @featurize
    def __rmod__(self, other: 'dsl.Operable') -> 'Modulus':
        return Modulus(other, self)


class Ordering(collections.namedtuple('Ordering', 'feature, direction')):
    """OrderBy spec."""

    feature: 'dsl.Operable'
    direction: 'dsl.Ordering.Direction'

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

        def __call__(self, feature: typing.Union['dsl.Operable', 'dsl.Ordering']) -> 'dsl.Ordering':
            if isinstance(feature, Ordering):
                feature = feature.feature
            return Ordering(feature, self)

    def __new__(
        cls, feature: 'dsl.Operable', direction: typing.Optional[typing.Union['dsl.Ordering.Direction', str]] = None
    ):
        return super().__new__(
            cls, Operable.ensure_is(feature), cls.Direction(direction) if direction else cls.Direction.ASCENDING
        )

    def __repr__(self):
        return f'{repr(self.feature)}{repr(self.direction)}'

    @classmethod
    def make(
        cls,
        specs: typing.Sequence[
            typing.Union[
                'dsl.Operable',
                typing.Union['dsl.Ordering.Direction', str],
                tuple['dsl.Operable', typing.Union['dsl.Ordering.Direction', str]],
            ]
        ],
    ) -> typing.Iterable['dsl.Ordering']:
        """Helper to generate orderings from given features and directions.

        Args:
            specs: One or many features or actual ordering instances.

        Returns:
            Sequence of ordering terms.
        """
        specs = itertools.zip_longest(specs, specs[1:])
        for feature, direction in specs:
            if isinstance(feature, Feature):
                if isinstance(direction, (Ordering.Direction, str)):
                    yield Ordering.Direction(direction)(feature)
                    next(specs)  # pylint: disable=stop-iteration-return
                else:
                    yield Ordering(feature)
            elif isinstance(feature, typing.Sequence) and len(feature) == 2:
                feature, direction = feature
                yield Ordering.Direction(direction)(feature)
            else:
                raise _exception.GrammarError('Expecting pair of feature and direction')


class Aliased(Feature):
    """Aliased feature representation."""

    operable: 'dsl.Operable' = property(opermod.itemgetter(0))
    name: str = property(opermod.itemgetter(1))

    def __new__(cls, feature: 'dsl.Feature', alias: str):
        return super().__new__(cls, feature.operable, alias)

    def __repr__(self):
        return f'{self.name}=[{repr(self.operable)}]'

    @property
    def kind(self) -> 'dsl.Any':
        """Feature type.

        Returns:
            Inner feature type.
        """
        return self.operable.kind

    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_aliased(self)


class Literal(Operable):
    """Literal value."""

    value: typing.Any = property(opermod.itemgetter(0))
    kind: 'dsl.Any' = property(opermod.itemgetter(1))

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

    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_literal(self)


class Element(Operable):
    """Named feature of particular origin (table or a reference)."""

    origin: 'dsl.Origin' = property(opermod.itemgetter(0))
    name: str = property(opermod.itemgetter(1))

    def __new__(cls, source: 'dsl.Origin', name: str):
        if isinstance(source, framod.Table) and not issubclass(cls, Column):
            return Column(source, name)
        return super().__new__(cls, source, name)

    def __repr__(self):
        return f'{repr(self.origin)}.{self.name}'

    @property
    def kind(self) -> 'dsl.Any':
        return self.origin.schema[self.name].kind

    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_element(self)


class Column(Element):
    """Special type of element is the table column type."""

    origin: 'dsl.Table' = property(opermod.itemgetter(0))

    def __new__(cls, table: 'dsl.Table', name: str):
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

    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        visitor.visit_expression(self)


class Univariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of just one argument/operand."""

    def __new__(cls, arg: 'dsl.Operable'):
        return super().__new__(cls, Operable.ensure_is(arg))


class Bivariate(Expression, metaclass=abc.ABCMeta):  # pylint: disable=abstract-method
    """Base class for functions/operators of two arguments/operands."""

    def __new__(cls, arg1: 'dsl.Operable', arg2: 'dsl.Operable'):
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

    left: 'dsl.Operable' = property(opermod.itemgetter(0))
    right: 'dsl.Operable' = property(opermod.itemgetter(1))

    def __repr__(self):
        return f'{repr(self[0])} {self.symbol} {repr(self[1])}'


class Prefix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for prefix operator expressions."""

    operand: 'dsl.Operable' = property(opermod.itemgetter(0))

    def __repr__(self):
        return f'{self.symbol} {repr(self[0])}'


class Postfix(Operator, Univariate, metaclass=abc.ABCMeta):
    """Base class for postfix operator expressions."""

    operand: 'dsl.Operable' = property(opermod.itemgetter(0))

    def __new__(cls, arg: 'dsl.Operable'):
        return super().__new__(cls, Operable.ensure_is(arg))

    def __repr__(self):
        return f'{repr(self[0])} {self.symbol}'


class Predicate(metaclass=abc.ABCMeta):
    """Base class for Logical and Comparison operators."""

    class Factors(typing.Mapping['dsl.Table', 'dsl.Factors']):
        """Mapping (read-only) of predicate factors to their tables. Factor is a predicate which is involving exactly
        one and only table.
        """

        def __init__(self, *predicates: 'dsl.Predicate'):
            items = {p: {f.origin for f in Column.dissect(p)} for p in predicates}
            if collections.Counter(len(s) == 1 for s in items.values())[True] != len(predicates):
                raise ValueError('Repeated or non-primitive predicates')
            self._items: typing.Mapping['dsl.Table', 'dsl.Predicate'] = types.MappingProxyType(
                {s.pop(): p for p, s in items.items()}
            )

        @classmethod
        def merge(
            cls,
            left: 'dsl.Predicate.Factors',
            right: 'dsl.Predicate.Factors',
            operator: typing.Callable[['dsl.Predicate', 'dsl.Predicate'], 'dsl.Predicate'],
        ) -> 'dsl.Predicate.Factors':
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

        def __and__(self, other: 'dsl.Predicate.Factors') -> 'dsl.Predicate.Factors':
            return self.merge(self, other, And)

        def __or__(self, other: 'dsl.Predicate.Factors') -> 'dsl.Predicate.Factors':
            return self.merge(self, other, Or)

        def __getitem__(self, table: 'dsl.Table') -> 'dsl.Predicate':
            return self._items[table]

        def __len__(self) -> int:
            return len(self._items)

        def __iter__(self) -> typing.Iterator['dsl.Table']:
            return iter(self._items)

    kind = kindmod.Boolean()

    @property
    @abc.abstractmethod
    def factors(self) -> 'dsl.Predicate.Factors':
        """Mapping of primitive source predicates - involving just a single Table.

        Returns:
            Break down of factors involved in this predicate.
        """

    @classmethod
    def ensure_is(cls: type['dsl.Operable'], feature: 'dsl.Operable') -> 'dsl.Operable':
        """Ensure given feature is a predicate. Since this mixin class is supposed to be used as a first base class of
        its feature implementors, this will mask the Feature.ensure_is API. Here we add special implementation depending
        on whether it is used directly on the Predicate class or its bare mixin subclasses or the actual Feature
        implementation using this mixin.

        Args:
            feature: 'dsl.Feature' instance to be checked for its compliance.

        Returns:
            Feature instance.
        """
        feature = Operable.ensure_is(feature)
        if cls is Predicate:  # bare Predicate - accept anything of a boolean kind.
            kindmod.Boolean.ensure(feature.kind)
        elif not issubclass(cls, Feature):  # bare Predicate mixin subclasses
            if not isinstance(feature, cls):
                raise _exception.GrammarError(f'{feature} not an instance of a {cls.__name__}')
        else:  # defer to the feature's .ensure_is implementation
            feature = next(b for b in cls.__bases__ if issubclass(b, Feature)).ensure_is(feature)
        return feature


class Logical(Predicate, metaclass=abc.ABCMeta):
    """Mixin for logical operators."""

    def __init__(self, *operands: 'dsl.Operable'):
        for arg in operands:
            Predicate.ensure_is(arg)


class And(Logical, Infix):
    """And operator."""

    symbol = 'AND'

    @functools.cached_property
    def factors(self: 'And') -> 'dsl.Predicate.Factors':
        return self.left.factors & self.right.factors


class Or(Logical, Infix):
    """Or operator."""

    symbol = 'OR'

    @functools.cached_property
    def factors(self: 'Or') -> 'dsl.Predicate.Factors':
        return self.left.factors | self.right.factors


class Not(Logical, Prefix):
    """Not operator."""

    symbol = 'NOT'

    @property
    def factors(self: 'Not') -> 'dsl.Predicate.Factors':
        return self.operand.factors


class Comparison(Predicate):
    """Mixin for comparison operators."""

    class Pythonic(Operable):
        """Semi proxy/lazy wrapper allowing native Python features like sorting or equality tests to work transparently
        without raising the syntax errors implemented in the constructors of the actual Comparison types.

        This instance is expected to be used only internally by Python itself. All code within ForML is supposed to use
        the extracted .operable instance of the true Comparison type.
        """

        operator: type[Infix] = property(opermod.itemgetter(0))
        left: 'dsl.Operable' = property(opermod.itemgetter(1))
        right: 'dsl.Operable' = property(opermod.itemgetter(2))

        def __new__(cls, operator: type[Infix], left: 'dsl.Operable', right: 'dsl.Operable'):
            return super().__new__(cls, operator, left, right)

        def __bool__(self):
            if self.operator is Equal:
                return hash(self.left) == hash(self.right)
            if self.operator is LessThan:
                return repr(self.left) < repr(self.right)
            raise RuntimeError(f'Unexpected Pythonic comparison using {self.operator}')

        @property
        def name(self) -> typing.Optional[str]:
            raise RuntimeError('Pythonic comparison proxy used as a feature')

        @property
        def kind(self) -> 'dsl.Any':
            raise RuntimeError('Pythonic comparison proxy used as a feature')

        def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
            raise RuntimeError('Pythonic comparison proxy used as a feature')

        @property
        def operable(self) -> Infix:
            """Materialize the real Comparison instance represented by this proxy.

            Returns:
                Comparison instance.
            """
            return self.operator(self.left, self.right)

    def __init__(self, *operands: 'dsl.Operable'):
        operands = [Operable.ensure_is(o) for o in operands]
        if not (
            all(kindmod.Numeric.match(o.kind) for o in operands) or all(o.kind == operands[0].kind for o in operands)
        ):
            raise _exception.GrammarError(f'Invalid operands for {self} comparison')

    @functools.cached_property
    def factors(self: 'Comparison') -> 'dsl.Predicate.Factors':
        return Predicate.Factors(self) if len({f.origin for f in Column.dissect(self)}) == 1 else Predicate.Factors()


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
        """Since this instance is also returned when python internally compares two Feature instances for equality (ie
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

    def __init__(self, *operands: 'dsl.Operable'):
        operands = [Operable.ensure_is(o) for o in operands]
        if not all(kindmod.Numeric.match(o.kind) for o in operands):
            raise _exception.GrammarError(f'Invalid arithmetic operands for {self}')

    @property
    def kind(self) -> 'dsl.Numeric':
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
    """Window type feature representation."""

    function: 'dsl.Window.Function' = property(opermod.itemgetter(0))
    partition: tuple['dsl.Operable'] = property(opermod.itemgetter(1))
    ordering: tuple['dsl.Ordering'] = property(opermod.itemgetter(2))
    frame: typing.Optional['dsl.Window.Frame'] = property(opermod.itemgetter(3))

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
            partition: typing.Sequence['dsl.Operable'],
            ordering: typing.Optional[
                typing.Sequence[
                    typing.Union[
                        Operable,
                        typing.Union['Ordering.Direction', str],
                        tuple['dsl.Operable', typing.Union['Ordering.Direction', str]],
                    ]
                ]
            ] = None,
            frame: typing.Optional = None,
        ) -> 'dsl.Window':
            """Create a window using this function.

            Args:
                partition: Window partitioning specifying the rows of query results.
                ordering: Order in which input rows should be processed.
                frame: Sliding window specification.

            Returns:
                Windowed feature instance.
            """
            return Window(self, partition, ordering, frame)

    def __new__(
        cls,
        function: 'dsl.Window.Function',
        partition: typing.Sequence['dsl.Feature'],
        ordering: typing.Optional[
            typing.Sequence[
                typing.Union[
                    'dsl.Operable',
                    typing.Union['dsl.Ordering.Direction', str],
                    tuple['dsl.Operable', typing.Union['dsl.Ordering.Direction', str]],
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
    def kind(self) -> 'dsl.Any':
        return self.function.kind

    def accept(self, visitor: 'dsl.Feature.Visitor') -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """
        visitor.visit_window(self)


class Aggregate(Cumulative, Window.Function, metaclass=abc.ABCMeta):
    """Base class for feature aggregation functions."""
