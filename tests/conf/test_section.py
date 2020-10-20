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
ForML section config unit tests.
"""
# pylint: disable=no-self-use
import abc
import re
import typing

import pytest

from forml import error
from forml.conf import section as secmod


class Parsed(metaclass=abc.ABCMeta):
    """Base class for parsed section tests using the test config from the config.toml.
    """
    class Section(secmod.Parsed):
        """Base class for parsed section fixtures.
        """
        INDEX = 'PARSED'  # referring to the section [PARSED] in the config.toml

        def __lt__(self, other: 'Parsed.Section') -> bool:
            # pylint: disable=no-member
            return sorted(set(self.params).difference(other.params)) < sorted(set(other.params).difference(self.params))

    @pytest.fixture(scope='session')
    def section(self) -> typing.Type['Parsed.Section']:
        """Section fixture.
        """
        return self.Section

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def invalid() -> str:
        """Invalid reference.
        """

    def test_invalid(self, section: typing.Type['Parsed.Section'], invalid: str):
        """Test the invalid parsing references.
        """
        with pytest.raises((error.Unexpected, error.Missing)):
            section.parse(invalid)

    def test_params(self, section: typing.Type['Parsed.Section']):
        """Test the params parsing.
        """
        assert section.parse('bar')[0].params == {'foo': 'baz'}


class TestSimple(Parsed):
    """Simple parser tests.
    """
    class Section(Parsed.Section):
        """CSV specified field list.
        """
        SELECTOR = 'simple'
        GROUP = 'SIMPLE'

    @staticmethod
    @pytest.fixture(scope='session', params=(',bar', 'bar, bar', 'blah'))
    def invalid(request) -> str:
        """Invalid reference.
        """
        return request.param


class TestComplex(Parsed):
    """SectionMeta unit tests.
    """
    class Section(Parsed.Section):
        """CSV specified field list.
        """
        PATTERN = re.compile(r'\s*(\w+)(?:\[(.*?)\])?\s*(?:,|$)')
        FIELDS = 'arg', 'params'
        SELECTOR = 'complex'
        GROUP = 'COMPLEX'

        @classmethod
        def extract(cls, reference: str, arg, *_) -> typing.Tuple[typing.Any]:
            """Custom argument extraction to allow for the arg parameter.
            """
            return arg, *super().extract(reference)

    @staticmethod
    @pytest.fixture(scope='session', params=(',baz[a, b]', 'bar]', 'blah'))
    def invalid(request) -> str:
        """Invalid reference.
        """
        return request.param

    def test_arg(self, section: typing.Type['Parsed.Section']):
        """Test the arg parsing.
        """
        assert section.parse('bar')[0].arg is None
        assert [p.arg for p in section.parse('foo[bar, baz], bar')] == ['bar, baz', None]
