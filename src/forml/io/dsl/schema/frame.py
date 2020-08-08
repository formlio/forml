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
import inspect
import logging
import operator
import typing
from typing import Any

from forml.io import etl
from forml.io.dsl import statement
from forml.io.dsl.schema import series

LOGGER = logging.getLogger(__name__)


class Visitor(metaclass=abc.ABCMeta):
    """Schema visitor.
    """
    @abc.abstractmethod
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


class Source(metaclass=abc.ABCMeta):
    """Source base class.
    """
    @property
    @abc.abstractmethod
    def columns(self) -> typing.Sequence[series.Element]:
        """Get the list of columns representing this source.

        Returns: Sequence of columns.
        """

    @abc.abstractmethod
    def accept(self, visitor: Visitor) -> None:
        """Visitor acceptor.

        Args:
            visitor: Visitor instance.
        """


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable sources.
    """
    @abc.abstractmethod
    def select(self, *columns: 'series.Column') -> 'statement.Query':
        """Specify the output columns to be provided.
        """

    @abc.abstractmethod
    def where(self, condition: 'series.Expression') -> 'statement.Query':
        """Add a row pre-filtering condition.
        """

    @abc.abstractmethod
    def having(self, condition: 'series.Expression') -> 'statement.Query':
        """Add a row post-filtering condition.
        """

    @abc.abstractmethod
    def join(self, other: Source, condition: 'series.Expression',
             kind: typing.Optional['statement.Join.Kind'] = None) -> 'statement.Query':
        """Join with other source.
        """

    @abc.abstractmethod
    def groupby(self, *columns: 'series.Element') -> 'statement.Query':
        """Aggregating spec.
        """

    @abc.abstractmethod
    def orderby(self, *columns: typing.Union[series.Element, typing.Union[
            'statement.Ordering.Direction', str], typing.Tuple[series.Element, typing.Union[
                'statement.Ordering.Direction', str]]]) -> 'statement.Query':
        """Ordering spec.
        """

    @abc.abstractmethod
    def limit(self, count: int, offset: int = 0) -> 'statement.Query':
        """Restrict the result rows by its max count with an optional offset.
        """

    def union(self, other: Source) -> 'statement.Query':
        """Set union with the other source.
        """
        return statement.Query(statement.Set(self, other, statement.Set.Kind.UNION))

    def intersection(self, other: Source) -> 'statement.Query':
        """Set intersection with the other source.
        """
        return statement.Query(statement.Set(self, other, statement.Set.Kind.INTERSECTION))

    def difference(self, other: Source) -> 'statement.Query':
        """Set difference with the other source.
        """
        return statement.Query(statement.Set(self, other, statement.Set.Kind.DIFFERENCE))


class Table(Queryable, tuple):
    """Table based source.

    This type can be used either as metaclass or as a base class to inherit from.
    """
    class Schema(type):
        """Meta class for schema type ensuring consistent hashing.
        """
        def __new__(mcs, name: str, bases: typing.Tuple[typing.Type], namespace: typing.Dict[str, typing.Any]) -> Any:
            existing = {s[k].name or k for s in bases if isinstance(s, Table.Schema) for k in s}
            for key, field in namespace.items():
                if not isinstance(field, etl.Field):
                    continue
                new = field.name or key
                if new in existing:
                    raise TypeError(f'Colliding field name {new} in schema {name}')
                existing.add(new)
            if bases and not existing:
                raise TypeError(f'No fields defined for schema {name}')
            return super().__new__(mcs, name, bases, namespace)

        def __hash__(cls):
            return hash(cls.__module__) ^ hash(cls.__qualname__)

        def __eq__(cls, other):
            return hash(cls) == hash(other)

        def __getitem__(cls, key: str) -> 'etl.Field':
            return getattr(cls, key)

        def __iter__(cls) -> typing.Iterator[str]:
            return iter(k for c in reversed(inspect.getmro(cls))
                        for k, f in c.__dict__.items() if isinstance(f, etl.Field))

    __schema__: typing.Type['etl.Schema'] = property(operator.itemgetter(0))

    def __new__(mcs, schema: typing.Union[str, typing.Type['etl.Schema']],  # pylint: disable=bad-classmethod-argument
                bases: typing.Optional[typing.Tuple[typing.Type]] = None,
                namespace: typing.Optional[typing.Dict[str, typing.Any]] = None):
        if issubclass(mcs, Table):  # used as metaclass
            if bases:
                bases = (bases[0].__schema__, )
            schema = mcs.Schema(schema, bases, namespace)
        else:
            if bases or namespace:
                raise TypeError('Unexpected use of schema table')
        return super().__new__(mcs, [schema])  # used as constructor

    def __hash__(self):
        return hash(self.__schema__)

    def __getattr__(self, name: str) -> 'series.Field':
        field: 'etl.Field' = self.__schema__[name]
        return series.Field(self, field.name or name, field.kind)

    @property
    def columns(self) -> typing.Sequence[series.Field]:
        return tuple(getattr(self, n) for n in self.__schema__)

    def accept(self, visitor: Visitor) -> None:
        visitor.visit_table(self)

    def select(self, *columns: 'series.Column') -> 'statement.Query':
        return statement.Query(self, columns)

    def where(self, condition: series.Expression) -> 'statement.Query':
        return statement.Query(self, prefilter=condition)

    def having(self, condition: series.Expression) -> 'statement.Query':
        return statement.Query(self, postfilter=condition)

    def join(self, other: Source, condition: series.Expression,
             kind: typing.Optional['statement.Join.Kind'] = None) -> 'statement.Query':
        return statement.Query(statement.Join(self, other, condition, kind))

    def groupby(self, *columns: 'series.Element') -> 'statement.Query':
        return statement.Query(self, grouping=columns)

    def orderby(self, *columns: typing.Union[series.Element, typing.Union[
            'statement.Ordering.Direction', str], typing.Tuple[series.Element, typing.Union[
                'statement.Ordering.Direction', str]]]) -> 'statement.Query':
        return statement.Query(self, ordering=columns)

    def limit(self, count: int, offset: int = 0) -> 'statement.Query':
        return statement.Query(self, rows=statement.Rows(count, offset))
