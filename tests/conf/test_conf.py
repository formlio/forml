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
# pylint: disable=protected-access,no-self-use
import pathlib
import types
import typing

import pytest

from forml import conf


def test_exists(cfg_file: pathlib.Path):
    """Test the config file exists.
    """
    assert cfg_file.is_file()


def test_src(cfg_file: pathlib.Path):
    """Test the registry config field.
    """
    assert cfg_file in conf.PARSER.sources


def test_get():
    """Test the get value matches the test config.toml
    """
    assert getattr(conf, 'foobar') == conf.foobar == 'baz'


class TestParser:
    """Parser unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def defaults() -> typing.Mapping[str, typing.Any]:
        """Default values fixtures.
        """
        return types.MappingProxyType({'foo': 'bar', 'baz': {'scalar': 1, 'seq': [10, 3, 'asd']}})

    @staticmethod
    @pytest.fixture(scope='function')
    def parser(defaults: typing.Mapping[str, typing.Any]) -> conf.Parser:
        """Parser fixture.
        """
        return conf.Parser(defaults)

    def test_update(self, parser: conf.Parser):
        """Parser update tests.
        """
        parser.update(baz={'another': 2})
        assert parser['baz']['scalar'] == 1
        parser.update({'baz': {'scalar': 3}})
        assert parser['baz']['scalar'] == 3
        assert parser['baz']['another'] == 2
        parser.update({'baz': {'seq': [3, 'qwe', 'asd']}})
        assert parser['baz']['seq'] == (3, 'qwe', 'asd', 10)

    def test_read(self, parser: conf.Parser, cfg_file: pathlib.Path):
        """Test parser file reading.
        """
        parser.read(cfg_file)
        assert cfg_file in parser.sources
