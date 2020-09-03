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

import pytest

from forml.io import etl
from forml.io.dsl import function
from forml.io.dsl.schema import kind
from forml.io.dsl.schema import series, frame


class Source(metaclass=abc.ABCMeta):
    """Source tests base class.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def source() -> frame.Queryable:
        """Undertest source.
        """

    def test_identity(self, source: frame.Queryable):
        """Test source identity.
        """
        hash(source)
        assert source != self

    def test_columns(self, source: frame.Queryable, student: frame.Table):
        """Test the reported column.
        """
        assert student.surname in source.columns
        assert source.surname == student.surname


class Queryable(Source, metaclass=abc.ABCMeta):
    """Base class for queryable tests.
    """
    def test_query(self, source: frame.Queryable):
        """Test query conversion.
        """
        assert isinstance(source.query, frame.Query)

    def test_select(self, source: frame.Queryable, student: frame.Table, school: frame.Table):
        """Select test.
        """
        assert source.select(student.score).selection[0] == student.score
        assert source.select(student.score, student.surname).selection == (student.score, student.surname)
        with pytest.raises(ValueError):
            source.select(school.name)  # school.name not part of source

    @classmethod
    def _expression(cls, source: frame.Queryable, student: frame.Table,
                    handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
                    target: typing.Callable[[frame.Query], series.Expression]):
        """Common element testing routine.
        """
        with pytest.raises(ValueError):
            handler(source, (student.score > 2).alias('foobar'))  # aliased
        assert target(handler(source, student.score > 2)) == function.GreaterThan(student.score, series.Literal(2))

    @classmethod
    def _condition(cls, source: frame.Queryable, student: frame.Table,
                   handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
                   target: typing.Callable[[frame.Query], series.Expression]):
        """Common condition testing routine.
        """
        cls._expression(source, student, handler, target)
        with pytest.raises(ValueError):
            handler(source, student.score + 1)  # not logical

    @classmethod
    def _subcondition(cls, source: frame.Queryable, student: frame.Table,
                      handler: typing.Callable[[frame.Queryable, series.Expression], frame.Query],
                      target: typing.Callable[[frame.Query], series.Expression]):
        """Common subcondition (cumulative condition) testing routine (for where/having).
        """
        cls._condition(source, student, handler, target)
        assert target(handler(handler(source, student.score > 2), student.level < 4)) == \
               function.And(function.LessThan(student.level, series.Literal(4)),
                            function.GreaterThan(student.score, series.Literal(2)))

    def test_where(self, source: frame.Queryable, student: frame.Table):
        """Where test.
        """
        self._subcondition(source, student, lambda s, e: s.where(e), lambda q: q.prefilter)

    def test_having(self, source: frame.Queryable, student: frame.Table):
        """Having test.
        """
        self._subcondition(source, student, lambda s, e: s.having(e), lambda q: q.postfilter)

    def test_join(self, source: frame.Queryable, student: frame.Table, school: frame.Table):
        """Join test.
        """
        joined = source.join(school, student.school == school.sid)
        assert isinstance(joined.source, frame.Join)
        assert joined.source.kind == frame.Join.Kind.LEFT
        assert joined.source.right == school
        assert joined.source.condition == function.Equal(student.school, school.sid)
        self._condition(source, student, lambda s, e: s.join(school, e), lambda q: q.source.condition)

    def test_groupby(self, source: frame.Queryable, student: frame.Table):
        """Groupby test.
        """
        assert source.groupby(student.score).grouping[0] == student.score
        assert source.groupby(student.score, student.surname).grouping == (student.score, student.surname)
        self._expression(source, student, lambda s, e: s.groupby(e), lambda q: q.grouping[0])

    def test_orderby(self, source: frame.Queryable, student: frame.Table):
        """Orderby test.
        """
        assert source.orderby(student.score).ordering[0] == (
            student.score, frame.Ordering.Direction.ASCENDING)
        assert source.orderby(frame.Ordering(student.score)).ordering[0] == (
            student.score, frame.Ordering.Direction.ASCENDING)
        assert source.orderby(frame.Ordering(
            student.score, frame.Ordering.Direction.DESCENDING)).ordering[0] == (
                student.score, frame.Ordering.Direction.DESCENDING)
        assert source.orderby(student.score, 'descending').ordering[0] == (
            student.score, frame.Ordering.Direction.DESCENDING)
        assert source.orderby(student.score, frame.Ordering.Direction.DESCENDING).ordering[0] == (
            student.score, frame.Ordering.Direction.DESCENDING)

        assert source.orderby(student.score, student.surname, 'descending').ordering == (
            (student.score, frame.Ordering.Direction.ASCENDING),
            (student.surname, frame.Ordering.Direction.DESCENDING))
        assert source.orderby(student.score, (student.surname, 'descending')).ordering == (
            (student.score, frame.Ordering.Direction.ASCENDING),
            (student.surname, frame.Ordering.Direction.DESCENDING))
        self._expression(source, student, lambda s, e: s.orderby(e), lambda q: q.ordering[0].column)

    def test_limit(self, source: frame.Queryable):
        """Limit test.
        """
        assert source.limit(1).rows == (1, 0)
        assert source.limit(1, 1).rows == (1, 1)


class TestSchema:
    """Table schema unit tests.
    """
    def test_empty(self):
        """Test empty schema with no fields.
        """
        with pytest.raises(TypeError):
            class Empty(etl.Schema):
                """Schema with no fields.
                """
            _ = Empty

    def test_colliding(self, student: frame.Table):
        """Test schema with colliding field names.
        """
        with pytest.raises(TypeError):
            class Colliding(student.__schema__):
                """Schema with colliding field names.
                """
                birthday = etl.Field(kind.Integer())
            _ = Colliding

    def test_access(self, student: frame.Table):
        """Test the schema access methods.
        """
        assert tuple(student.__schema__) == ('surname', 'dob', 'level', 'score', 'school')
        assert student.dob.name == 'birthday'


class TestTable(Queryable):
    """Table unit tests.
    """
    def test_fields(self, student: frame.Table):
        """Fields getter tests.
        """
        assert student.dob.name == 'birthday'
        assert student.score.name == 'score'
        with pytest.raises(AttributeError):
            _ = student.xyz

    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Queryable:
        return student


class TestQuery(Queryable):
    """Query unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def source(student: frame.Table) -> frame.Source:
        """Source fixture.
        """
        return frame.Query(student)
