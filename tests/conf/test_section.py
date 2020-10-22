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
import typing

import pytest

from forml import error
from forml.conf import section as secmod


class Resolved(metaclass=abc.ABCMeta):
    """Base class for parsed section tests using the test config from the config.toml.
    """
    class Section(secmod.Resolved):
        """Base class for parsed section fixtures.
        """
        INDEX = 'RESOLVED'  # referring to the section [RESOLVED] in the config.toml

        def __lt__(self, other: 'Resolved.Section') -> bool:
            # pylint: disable=no-member
            return sorted(set(self.params).difference(other.params)) < sorted(set(other.params).difference(self.params))

    @pytest.fixture(scope='session')
    def section(self) -> typing.Type['Resolved.Section']:
        """Section fixture.
        """
        return self.Section

    @staticmethod
    @pytest.fixture(scope='session')
    @abc.abstractmethod
    def invalid() -> str:
        """Invalid reference.
        """

    def test_invalid(self, section: typing.Type['Resolved.Section'], invalid: str):
        """Test the invalid parsing references.
        """
        with pytest.raises(error.Invalid):
            section.resolve(invalid)

    def test_default(self, section: typing.Type['Resolved.Section']):
        """Test the default resolving.
        """
        assert section.default


class TestSingle(Resolved):
    """Single parser tests.
    """
    class Section(Resolved.Section):
        """Single field value.
        """
        SELECTOR = 'single'
        GROUP = 'SINGLE'

    @staticmethod
    @pytest.fixture(scope='session', params=('baz', ))
    def invalid(request) -> str:
        """Invalid reference.
        """
        return request.param

    def test_params(self, section: typing.Type['Resolved.Section']):
        """Test the params parsing.
        """
        assert section.resolve('bar').params == {'foo': 'baz'}


class TestMulti(Resolved):
    """SectionMeta unit tests.
    """
    class Section(secmod.Multi, Resolved.Section):
        """Field list.
        """
        SELECTOR = 'multi'
        GROUP = 'MULTI'

    @staticmethod
    @pytest.fixture(scope='session', params=('blah', ['blah'], ['blah', 'baz']))
    def invalid(request) -> str:
        """Invalid reference.
        """
        return request.param

    def test_params(self, section: typing.Type['Resolved.Section']):
        """Test the arg parsing.
        """
        assert section.resolve('bar')[0].params == {'foo': 'baz'}
        assert [r.params for r in section.resolve(['bar', 'foo'])] == [{'baz': 'foo'}, {'foo': 'baz'}]
