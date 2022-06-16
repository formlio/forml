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
ETL kinds unit tests.
"""
import abc
import datetime
import decimal
import typing

import pytest

from forml.io.dsl._struct import kind as kindmod


def test_reflect():
    """Test reflection exceptions."""
    with pytest.raises(ValueError):
        kindmod.reflect([])  # empty array
    with pytest.raises(ValueError):
        kindmod.reflect({})  # empty map
    with pytest.raises(ValueError):
        kindmod.reflect({1: 'a', 2: 3})  # not a struct (int keys) neither map (multiple value types)


class Data(metaclass=abc.ABCMeta):
    """Common test base class."""

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def kind() -> type[kindmod.Any]:
        """Undertest kind type."""

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def sample() -> typing.Any:
        """Undertest value."""

    def test_reflect(self, sample: typing.Any, kind: type[kindmod.Any]):
        """Value kind reflection test."""
        assert kindmod.reflect(sample) == kind()

    def test_hashable(self, kind: type[kindmod.Any]):
        """Test hashability."""
        assert hash(kind()) == hash(kind())

    def test_subkinds(self, kind: type[kindmod.Any]):
        """Test the kind is recognized as data subkind."""
        assert type(kind()) in kindmod.Any.__subkinds__

    def test_cardinality(self, kind: type[kindmod.Any]):
        """Test the kind cardinality."""
        assert kind().__cardinality__ >= 0


class Primitive(Data, metaclass=abc.ABCMeta):
    """Primitive kind test base class."""

    def test_singleton(self, kind: type[kindmod.Any]):
        """Test the instances are singletons."""
        assert kind() is kind()


class Compound(Data, metaclass=abc.ABCMeta):
    """Compound kind test base class."""


class TestBoolean(Primitive):
    """Boolean type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(True, False))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Boolean


class TestInteger(Primitive):
    """Integer type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(1, -1, 0))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Integer


class TestFloat(Primitive):
    """Float type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(1.1, -1.1, 0.1))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Float


class TestString(Primitive):
    """String type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=('foo', ''))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.String


class TestDecimal(Primitive):
    """Decimal type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(decimal.Decimal('1.1'), decimal.Decimal(0)))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Decimal


class TestTimestamp(Primitive):
    """Timestamp type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(datetime.datetime.utcfromtimestamp(0), datetime.datetime(2020, 5, 5, 10)))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Timestamp


class TestDate(Primitive):
    """Date type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session', params=(datetime.date.fromtimestamp(0), datetime.date(2020, 5, 5)))
    def sample(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return kindmod.Date


class TestArray(Compound):
    """Array type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sample() -> typing.Any:
        return tuple([1, 2, 3])

    @staticmethod
    @pytest.fixture(scope='session')
    def element() -> kindmod.Any:
        """Element fixture."""
        return kindmod.Integer()

    @staticmethod
    @pytest.fixture(scope='session')
    def kind(element: kindmod.Any) -> type[kindmod.Any]:
        return lambda: kindmod.Array(element)

    def test_attribute(self, kind: type[kindmod.Any], element: kindmod.Any):
        """Test attribute access."""
        assert kind().element == element


class TestMap(Compound):
    """Map type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sample() -> typing.Any:
        return {1: 'a', 2: 'b', 3: 'c'}

    @staticmethod
    @pytest.fixture(scope='session')
    def key() -> kindmod.Any:
        """Key fixture."""
        return kindmod.Integer()

    @staticmethod
    @pytest.fixture(scope='session')
    def value() -> kindmod.Any:
        """Value fixture."""
        return kindmod.String()

    @staticmethod
    @pytest.fixture(scope='session')
    def kind(key: kindmod.Any, value: kindmod.Any) -> type[kindmod.Any]:
        return lambda: kindmod.Map(key, value)

    def test_attribute(self, kind: type[kindmod.Any], key: kindmod.Any, value: kindmod.Any):
        """Test attribute access."""
        instance = kind()
        assert instance.key == key
        assert instance.value == value


class TestStruct(Compound):
    """Struct type unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def sample() -> typing.Any:
        return {'foo': 1, 'bar': 'blah', 'baz': True}

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> type[kindmod.Any]:
        return lambda: kindmod.Struct(foo=kindmod.Integer(), bar=kindmod.String(), baz=kindmod.Boolean())
