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
# pylint: disable=no-self-use

import abc
import typing

import cloudpickle
import pytest

from forml.io.dsl import function, error, struct
from forml.io.dsl.struct import series, frame, kind


class Source(metaclass=abc.ABCMeta):
    """Source tests base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def source() -> frame.Queryable:
        """Undertest source."""

    def test_identity(self, source: frame.Source, school: frame.Table):
        """Test source identity."""
        assert len({source, source, school}) == 2

    def test_serilizable(self, source: frame.Source):
        """Test source serializability."""
        assert cloudpickle.loads(cloudpickle.dumps(source)) == source

    def test_columns(self, source: frame.Source, student: frame.Table):
        """Test the reported column."""
        assert student.surname in source.columns
        assert source.surname == student.surname
        assert source['surname'] == student['surname']
        assert source.birthday == student.birthday
        assert source['birthday'] == student['birthday']
        with pytest.raises(AttributeError):
            _ = student.xyz
        with pytest.raises(KeyError):
            _ = student['xyz']

    def test_schema(self, source: frame.Source):
        """Test the reported schema."""
        assert issubclass(source.schema, struct.Schema)


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable tests."""

    def test_query(self, source: frame.Queryable):
        """Test query conversion."""
        assert isinstance(source.query, frame.Query)

    def test_instance(self, source: frame.Source, student: frame.Table):
        """Test the queryable instance."""
        assert source.instance.query == student.query

    def test_reference(self, source: frame.Queryable):
        """Test the queryable reference."""
        assert isinstance(source.reference(), frame.Reference)

    def test_select(self, source: frame.Queryable):
        """Select test."""
        assert source.select(source.score).selection[0] == source.score
        assert source.select(source.score, source.surname).selection == (source.score, source.surname)
        with pytest.raises(error.Syntax):
            source.select(source.reference().score)  # not subset of source columns

    @classmethod
    def _expression(
        cls,
        source: frame.Queryable,
        handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
        target: typing.Callable[[frame.Query], series.Expression],
    ):
        """Common element testing routine."""
        score = source.query.source.score
        with pytest.raises(error.Syntax):
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
        with pytest.raises(error.Syntax):
            handler(source, source.score + 1)  # not logical
        with pytest.raises(error.Syntax):  # not subset of source columns
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
        with pytest.raises(error.Syntax):
            source.where(function.Count(source.score) > 2)  # aggregation filter

    def test_having(self, source: frame.Queryable):
        """Having test."""
        self._subcondition(source, lambda s, e: s.having(e), lambda q: q.postfilter)
        assert source.having(function.Count(source.score) > 2)  # aggregation filtering is valid for having

    def test_join(self, source: frame.Queryable, school: frame.Table):
        """Join test."""
        joined = source.join(school, school.sid == source.school)
        assert isinstance(joined.source, frame.Join)
        assert joined.source.kind == frame.Join.Kind.INNER
        assert joined.source.right == school
        assert joined.source.condition == function.Equal(school.sid, source.school)
        self._condition(source, lambda s, e: s.join(school, e), lambda q: q.source.condition)
        with pytest.raises(error.Syntax):
            source.join(school, function.Count(source.score) > 2)  # aggregation filter

    def test_groupby(self, source: frame.Queryable):
        """Groupby test."""
        assert source.select(source.score.alias('foo')).groupby(source.score).grouping[0] == source.score
        assert source.select(source.score, source.surname).groupby(source.score, source.surname).grouping == (
            source.score,
            source.surname,
        )
        with pytest.raises(error.Syntax):
            source.select(source.score, source.surname).groupby(source.score)  # surname neither aggregate nor group
        with pytest.raises(error.Syntax):
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
        self._expression(source, lambda s, e: s.orderby(e), lambda q: q.ordering[0].column)

    def test_limit(self, source: frame.Queryable):
        """Limit test."""
        assert source.limit(1).rows == (1, 0)
        assert source.limit(1, 1).rows == (1, 1)


class Tangible(Queryable, metaclass=abc.ABCMeta):
    """Base class for tangible frames."""

    def test_columns(self, source: frame.Origin, student: frame.Table):
        assert all(isinstance(c, series.Element) for c in source.columns)
        assert student.dob.name == 'birthday'
        assert student.score.name == 'score'


class TestSchema:
    """Table schema unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def schema(student: frame.Table) -> typing.Type['struct.Schema']:
        """Schema fixture."""
        return student.schema

    def test_identity(self, schema: typing.Type['struct.Schema'], student: frame.Table):
        """Schema identity tests."""
        other = student.query.schema
        assert schema is not other
        assert len({schema, other}) == 1

    def test_colliding(self, schema: typing.Type['struct.Schema']):
        """Test schema with colliding field names."""
        with pytest.raises(error.Syntax):

            class Colliding(schema):
                """Schema with colliding field names."""

                birthday = struct.Field(kind.Integer())

            _ = Colliding

    def test_access(self, schema: typing.Type['struct.Schema']):
        """Test the schema access methods."""
        assert tuple(schema) == ('surname', 'dob', 'level', 'score', 'school')
        assert schema.dob.name == 'birthday'
        assert schema['dob'].name == 'birthday'
        assert schema['birthday'].name == 'birthday'


class TestReference(Tangible):
    """Table unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Reference:
        return student.reference()


class TestTable(Tangible):
    """Table unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Table:
        return student

    def test_columns(self, source: frame.Origin, student: frame.Table):
        Queryable.test_columns(self, source, student)
        Tangible.test_columns(self, source, student)


class TestQuery(Queryable):
    """Query unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Query:
        """Source fixture."""
        return student.query
