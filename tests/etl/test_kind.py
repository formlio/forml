"""
ETL kinds unit tests.
"""
# pylint: disable=no-self-use
import abc
import datetime
import decimal
import typing

import pytest

from forml.etl import kind as kindmod


def test_reflect():
    """Test reflection exceptions.
    """
    with pytest.raises(ValueError):
        kindmod.reflect(list())  # empty array
    with pytest.raises(ValueError):
        kindmod.reflect(dict())  # empty map
    with pytest.raises(ValueError):
        kindmod.reflect({1: 'a', 2: 3})  # not a struct (int keys) neither map (multiple value types)


class Data(metaclass=abc.ABCMeta):
    """Common test base class.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def kind() -> typing.Type[kindmod.Data]:
        """Undertest kind type.
        """

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def value() -> typing.Any:
        """Undertest value.
        """

    def test_reflect(self, value: typing.Any, kind: typing.Type[kindmod.Data]):
        """Value kind reflection test.
        """
        assert kindmod.reflect(value) == kind()


class Primitive(Data, metaclass=abc.ABCMeta):
    """Primitive kind test base class.
    """
    def test_singleton(self, kind: typing.Type[kindmod.Data]):
        """Test the instances are singletons.
        """
        assert kind() is kind()


class Compound(Data, metaclass=abc.ABCMeta):
    """Compound kind test base class.
    """


class TestBoolean(Primitive):
    """Boolean type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(True, False))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Boolean


class TestInteger(Primitive):
    """Integer type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(1, -1, 0))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Integer


class TestFloat(Primitive):
    """Float type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(1.1, -1.1, 0.1))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Float


class TestString(Primitive):
    """String type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=('foo', ''))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.String


class TestDecimal(Primitive):
    """Decimal type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(decimal.Decimal('1.1'), decimal.Decimal(0)))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Decimal


class TestTimestamp(Primitive):
    """Timestamp type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(datetime.datetime.utcfromtimestamp(0), datetime.datetime(2020, 5, 5, 10)))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Timestamp


class TestDate(Primitive):
    """Date type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session', params=(datetime.date.fromtimestamp(0), datetime.date(2020, 5, 5)))
    def value(request) -> typing.Any:
        return request.param

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return kindmod.Date


class TestArray(Compound):
    """Array type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def value() -> typing.Any:
        return tuple([1, 2, 3])

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return lambda: kindmod.Array(kindmod.Integer())


class TestMap(Compound):
    """Map type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def value() -> typing.Any:
        return {1: 'a', 2: 'b', 3: 'c'}

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return lambda: kindmod.Map(kindmod.Integer(), kindmod.String())


class TestStruct(Compound):
    """Struct type unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def value() -> typing.Any:
        return {'foo': 1, 'bar': 'blah', 'baz': True}

    @staticmethod
    @pytest.fixture(scope='session')
    def kind() -> typing.Type[kindmod.Data]:
        return lambda: kindmod.Struct(foo=kindmod.Integer(), bar=kindmod.String(), baz=kindmod.Boolean())
