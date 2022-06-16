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
ETL unit tests.
"""

import abc
import pickle
import typing

import cloudpickle
import pytest

from forml.io import dsl
from forml.io.dsl import _struct, function
from forml.io.dsl._struct import frame, kind, series


class Source(metaclass=abc.ABCMeta):
    """Source tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def source() -> frame.Queryable:
        """Undertest source."""

    def test_identity(self, source: frame.Source, school_table: frame.Table):
        """Test source identity."""
        assert len({source, source, school_table}) == 2

    def test_serilizable(self, source: frame.Source):
        """Test source serializability."""
        assert cloudpickle.loads(cloudpickle.dumps(source)) == source
        assert pickle.loads(pickle.dumps(source.schema)) == source.schema

    def test_features(self, source: frame.Source, student_table: frame.Table):
        """Test the reported feature."""
        assert student_table.surname in source.features
        assert source.surname == student_table.surname
        assert source['surname'] == student_table['surname']
        assert source.birthday == student_table.birthday
        assert source['birthday'] == student_table['birthday']
        with pytest.raises(AttributeError):
            _ = student_table.xyz
        with pytest.raises(KeyError):
            _ = student_table['xyz']

    def test_schema(self, source: frame.Source):
        """Test the reported schema."""
        assert issubclass(source.schema, _struct.Schema)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable tests."""

    def test_query(self, source: frame.Queryable):
        """Test query conversion."""
        assert isinstance(source.query, frame.Query)

    def test_instance(self, source: frame.Source, student_table: frame.Table):
        """Test the queryable instance."""
        assert source.instance.query == student_table.query

    def test_reference(self, source: frame.Queryable):
        """Test the queryable reference."""
        assert isinstance(source.reference(), frame.Reference)

    def test_select(self, source: frame.Queryable):
        """Select test."""
        assert source.select(source.score).selection[0] == source.score
        assert source.select(source.score, source.surname).selection == (source.score, source.surname)
        with pytest.raises(dsl.GrammarError):
            source.select(source.reference().score)  # not subset of source features

    @classmethod
    def _expression(
        cls,
        source: frame.Queryable,
        handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
        target: typing.Callable[[frame.Query], series.Expression],
    ):
        """Common element testing routine."""
        score = source.query.source.score
        with pytest.raises(dsl.GrammarError):
            handler(source, (score > 2).alias('foobar'))  # aliased
        assert target(handler(source, score > 2)) == function.GreaterThan(score, series.Literal(2))

    @classmethod
    def _condition(
        cls,
        source: frame.Queryable,
        handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
        target: typing.Callable[[frame.Query], series.Expression],
    ):
        """Common condition testing routine."""
        cls._expression(source, handler, target)
        with pytest.raises(dsl.GrammarError):
            handler(source, source.score + 1)  # not logical
        with pytest.raises(dsl.GrammarError):  # not subset of source features
            handler(source, source.reference().score == 'foo')

    @classmethod
    def _subcondition(
        cls,
        source: frame.Queryable,
        handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
        target: typing.Callable[[frame.Query], series.Expression],
    ):
        """Common subcondition (cumulative condition) testing routine (for where/having)."""
        cls._condition(source, handler, target)
        assert target(handler(handler(source, source.score > 2), source.level < 4)) == function.And(
            function.LessThan(source.level, series.Literal(4)), function.GreaterThan(source.score, series.Literal(2))
        )

    def test_where(self, source: frame.Queryable):
        """Where test."""
        self._subcondition(source, lambda s, e: s.where(e), lambda q: q.prefilter)
        with pytest.raises(dsl.GrammarError):
            source.where(function.Count(source.score) > 2)  # aggregation filter

    def test_having(self, source: frame.Queryable):
        """Having test."""
        self._subcondition(source, lambda s, e: s.having(e), lambda q: q.postfilter)
        assert source.having(function.Count(source.score) > 2)  # aggregation filtering is valid for having

    def test_join(self, source: frame.Queryable, school_table: frame.Table):
        """Join test."""
        joined = source.join(school_table, school_table.sid == source.school)
        assert isinstance(joined.source, frame.Join)
        assert joined.source.kind == frame.Join.Kind.INNER
        assert joined.source.right == school_table
        assert joined.source.condition == function.Equal(school_table.sid, source.school)
        self._condition(source, lambda s, e: s.join(school_table, e), lambda q: q.source.condition)
        with pytest.raises(dsl.GrammarError):
            source.join(school_table, function.Count(source.score) > 2)  # aggregation filter

    def test_groupby(self, source: frame.Queryable):
        """Groupby test."""
        assert source.select(source.score.alias('foo')).groupby(source.score).grouping[0] == source.score
        assert source.select(source.score, source.surname).groupby(source.score, source.surname).grouping == (
            source.score,
            source.surname,
        )
        with pytest.raises(dsl.GrammarError):
            source.select(source.score, source.surname).groupby(source.score)  # surname neither aggregate nor group
        with pytest.raises(dsl.GrammarError):
            source.select(function.Count(source.score)).groupby(function.Count(source.score))  # grouping by aggregation
        assert source.select(source.score, function.Count(source.surname)).groupby(source.score)
        assert source.select(source.score, function.Count(source.surname) + 1).groupby(source.score)
        self._expression(source.select(source.score > 2), lambda s, e: s.groupby(e), lambda q: q.grouping[0])

    def test_orderby(self, source: frame.Queryable):
        """Orderby test."""
        assert source.orderby(source.score).ordering[0] == (source.score, series.Ordering.Direction.ASCENDING)
        assert source.orderby(source.score, source.surname, 'desc').ordering == (
            (source.score, series.Ordering.Direction.ASCENDING),
            (source.surname, series.Ordering.Direction.DESCENDING),
        )
        assert source.orderby(source.score, 'DESC', (source.surname, 'ASCENDING')).ordering == (
            (source.score, series.Ordering.Direction.DESCENDING),
            (source.surname, series.Ordering.Direction.ASCENDING),
        )
        self._expression(source, lambda s, e: s.orderby(e), lambda q: q.ordering[0].feature)

    def test_limit(self, source: frame.Queryable):
        """Limit test."""
        assert source.limit(1).rows == (1, 0)
        assert source.limit(1, 1).rows == (1, 1)


class Tangible(Queryable, metaclass=abc.ABCMeta):
    """Base class for tangible frames."""

    def test_features(self, source: frame.Origin, student_table: frame.Table):
        assert all(isinstance(c, series.Element) for c in source.features)
        assert student_table.dob.name == 'birthday'
        assert student_table.score.name == 'score'


class TestSchema:
    """Table schema unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def schema(student_table: dsl.Table) -> dsl.Source.Schema:
        """Schema fixture."""
        return student_table.schema

    def test_identity(self, schema: dsl.Source.Schema, student_table: dsl.Table):
        """Schema identity tests."""
        other = student_table.query.schema
        assert schema is not other
        assert len({schema, other}) == 1

    def test_colliding(self, schema: dsl.Source.Schema):
        """Test schema with colliding field names."""

        class Base(_struct.Schema):
            """Another schema also having the birthday field."""

            birthday = _struct.Field(kind.Integer())

        with pytest.raises(dsl.GrammarError, match='Colliding base classes'):

            class BaseCollision(schema, Base.schema):  # pylint: disable=inherit-non-class
                """Schema with colliding base classes."""

            _ = BaseCollision

        with pytest.raises(dsl.GrammarError, match='Colliding field name'):

            class FieldCollision(schema):
                """Schema with colliding field names."""

                birthday = _struct.Field(kind.Integer())

            _ = FieldCollision

        class Override(schema):
            """Schema with overridden field kind."""

            school = _struct.Field(kind.String())

        assert schema.school.kind == kind.Integer() and Override.school.kind == kind.String()

    def test_access(self, schema: dsl.Source.Schema):
        """Test the schema access methods."""
        assert tuple(f.name for f in schema) == ('surname', 'birthday', 'level', 'score', 'school', 'updated')
        assert schema.dob.name == 'birthday'
        assert schema['dob'].name == 'birthday'
        assert schema['birthday'].name == 'birthday'

    def test_ordering(self):
        """Test the field ordering rules in schema inheritance."""

        class Base(_struct.Schema):
            """Base schema."""

            first = _struct.Field(kind.Integer())
            fixme = _struct.Field(kind.Float(), name='old')

        class Child(Base):
            """Child schema - adding a field "last" plus overriding kind of the "fixme" field."""

            last = _struct.Field(kind.Integer())
            fixme = _struct.Field(kind.String(), name='new')

        assert tuple(f.name for f in Child.schema) == ('first', 'new', 'last')  # pylint: disable=not-an-iterable
        assert Child.fixme.kind == kind.String()
        assert Child.new == Child.fixme
        assert Base.old
        with pytest.raises(AttributeError):
            assert Child.old

    def test_serializable(self, schema: dsl.Source.Schema):
        """Test schema serializability."""
        assert pickle.loads(pickle.dumps(schema)) == schema


class TestReference(Tangible):
    """Table unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student_table: dsl.Table) -> frame.Reference:
        return student_table.reference()


class TestTable(Tangible):
    """Table unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student_table: dsl.Table) -> frame.Table:
        return student_table

    def test_features(self, source: frame.Origin, student_table: dsl.Table):
        Queryable.test_features(self, source, student_table)
        Tangible.test_features(self, source, student_table)


class TestQuery(Queryable):
    """Query unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student_table: dsl.Table) -> frame.Query:
        """Source fixture."""
        return student_table.query
