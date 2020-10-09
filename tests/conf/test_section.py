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
# pylint: disable=protected-access,no-self-use
import re
import types
import typing

import pytest

from forml import error
from forml.conf import section as secmod  # pylint: disable=unused-import


class Parsed(secmod.Parsed):
    """Base class for parsed fixtures.
    """
    REFEREE = 'FOOBAR'


class TestParsed:
    """SectionMeta unit tests.
    """
    def test_simple(self, conf: types.ModuleType):  # pylint: disable=unused-argument
        """Test with default pattern.
        """
        class Simple(Parsed):
            """CSV specified field list.
            """
            SELECTOR = 'foo'

        # pylint: disable=no-member
        assert Simple.parse('bar')[0].kwargs['bar'] == 'foo'
        with pytest.raises(error.Unexpected):
            Simple.parse(',bar')
        with pytest.raises(error.Unexpected):
            Simple.parse('bar, bar')

    def test_complex(self, conf: types.ModuleType):  # pylint: disable=unused-argument
        """Test with complex patter, custom fields and extract method.
        """
        class Complex(Parsed):
            """Complex specified field list.
            """
            PATTERN = re.compile(r'\s*(\w+)(?:\[(.*?)\])?\s*(?:,|$)')
            FIELDS = 'foo', 'kwargs'
            SELECTOR = 'bar'

            @classmethod
            def extract(cls, reference: str, arg, *_) -> typing.Tuple[typing.Any]:
                return arg, *super().extract(reference)

        # pylint: disable=no-member
        assert Complex.parse('bar') == ((None, {'bar': 'bar'}), )
        assert Complex.parse('foo[bar, baz], bar') == (('bar, baz', {'foo': 'bar'}), (None, {'bar': 'bar'}))
        with pytest.raises(error.Unexpected):
            Complex.parse(',baz[a, b]')
