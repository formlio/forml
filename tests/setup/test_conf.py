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
ForML config unit tests.
"""
import abc
import pathlib
import types
import typing

import pytest

import forml
from forml.setup import _conf


def test_exists(cfg_file: pathlib.Path):
    """Test the config file exists."""
    assert cfg_file.is_file()


def test_src(cfg_file: pathlib.Path):
    """Test the registry config field."""
    assert cfg_file in _conf.CONFIG.sources


def test_get():
    """Test the get value matches the test config.toml"""
    assert _conf.CONFIG['foobar'] == 'baz'


def test_defaults():
    """Test the static defaults."""
    assert _conf.CONFIG[_conf.SECTION_LOGGING][_conf.OPT_CONFIG] == 'logging.ini'
    assert _conf.CONFIG[_conf.SECTION_TEMPLATING][_conf.OPT_PATH] == 'templates'
    assert _conf.CONFIG[_conf.SECTION_TEMPLATING][_conf.OPT_DEFAULT] == 'default'
    assert _conf.CONFIG[_conf.OPT_TMPDIR]


class TestConfig:
    """Parser unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def defaults() -> typing.Mapping[str, typing.Any]:
        """Default values fixtures."""
        return types.MappingProxyType({'foo': 'bar', 'baz': {'scalar': 1, 'seq': [10, 3, 'asd']}})

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(defaults: typing.Mapping[str, typing.Any]) -> _conf.Config:
        """Parser fixture."""
        return _conf.Config(defaults)

    def test_update(self, parser: _conf.Config):
        """Parser update tests."""
        parser.update(baz={'another': 2})
        assert parser['baz']['scalar'] == 1
        parser.update({'baz': {'scalar': 3}})
        assert parser['baz']['scalar'] == 3
        assert parser['baz']['another'] == 2
        parser.update({'baz': {'seq': [3, 'qwe', 'asd']}})
        assert parser['baz']['seq'] == (3, 'qwe', 'asd', 10)

    def test_read(self, parser: _conf.Config, cfg_file: pathlib.Path):
        """Test parser file reading."""
        parser.read(cfg_file)
        assert cfg_file in parser.sources


class Resolved(metaclass=abc.ABCMeta):
    """Base class for parsed section tests using the test config from the config.toml."""

    class Section(_conf.Section):
        """Base class for parsed section fixtures."""

        FIELDS: tuple[str] = ('blah', 'params')
        INDEX: str = 'RESOLVED'  # referring to the section [RESOLVED] in the config.toml

        @classmethod
        def _extract(
            cls, reference: str, kwargs: typing.Mapping[str, typing.Any]
        ) -> tuple[typing.Sequence[typing.Any], typing.Mapping[str, typing.Any]]:
            kwargs = dict(kwargs)
            blah = kwargs.pop('blah', None)
            _, kwargs = super()._extract(reference, kwargs)
            return [blah], kwargs

        def __lt__(self, other: 'Resolved.Section') -> bool:
            # pylint: disable=no-member
            return sorted(set(self.params).difference(other.params)) < sorted(set(other.params).difference(self.params))

    @pytest.fixture(scope='session')
    def section(self) -> type['Resolved.Section']:
        """Section fixture."""
        return self.Section

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def invalid() -> str:
        """Invalid reference."""

    def test_invalid(self, section: type['Resolved.Section'], invalid: str):
        """Test the invalid parsing references."""
        with pytest.raises(forml.InvalidError):
            section.resolve(invalid)

    def test_default(self, section: type['Resolved.Section']):
        """Test the default resolving."""
        assert section.default


class TestSingle(Resolved):
    """Single parser tests."""

    class Section(Resolved.Section):
        """Single field value."""

        SELECTOR = 'single'
        GROUP = 'SINGLE'

    @staticmethod
    @pytest.fixture(scope='session', params=('baz',))
    def invalid(request) -> str:
        """Invalid reference."""
        return request.param

    def test_params(self, section: type['Resolved.Section']):
        """Test the params parsing."""
        parsed = section.resolve('bar')
        assert parsed.blah == 'single'
        assert parsed.params == {'foo': 'baz', 'blah': 'bar', 'bar': 'blah'}


class TestMulti(Resolved):
    """SectionMeta unit tests."""

    class Section(_conf.Multi, Resolved.Section):
        """Field list."""

        SELECTOR = 'multi'
        GROUP = 'MULTI'

    @staticmethod
    @pytest.fixture(scope='session', params=('blah', ['blah'], ['blah', 'baz']))
    def invalid(request) -> str:
        """Invalid reference."""
        return request.param

    def test_params(self, section: type['Resolved.Section']):
        """Test the arg parsing."""
        assert section.resolve('bar')[0].params == {'foo': 'baz'}
        assert [r.params for r in section.resolve(['bar', 'foo'])] == [{'baz': 'foo'}, {'foo': 'baz'}]
