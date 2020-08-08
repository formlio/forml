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
import types

import pytest

from forml import error
from forml.conf import section as secmod  # pylint: disable=unused-import


class TestMeta:
    """SectionMeta unit tests.
    """
    def test_simple(self, conf: types.ModuleType):  # pylint: disable=unused-argument
        """Test with default pattern.
        """
        class Simple(metaclass=secmod.Meta):  # pylint: disable=undefined-variable
            """CSV specified field list.
            """
            FIELDS = 'foo'

        # pylint: disable=no-member
        assert Simple.parse('') == ()
        assert Simple.parse('bar')[0].foo == 'bar'
        assert [s.foo for s in Simple.parse('bar, baz')] == ['bar', 'baz']
        with pytest.raises(error.Unexpected):
            Simple.parse(',bar')
        with pytest.raises(error.Unexpected):
            Simple.parse('bar, bar')

    def test_complex(self, conf: types.ModuleType):  # pylint: disable=unused-argument
        """Test for the secmod.Meta metaclass with default pattern.
        """
        class Complex(metaclass=secmod.Meta):  # pylint: disable=undefined-variable
            """Complex specified field list.
            """
            PATTERN = r'\s*(\w+)(?:\[(.*?)\])?\s*(?:,|$)'
            FIELDS = 'foo, bar'

        # pylint: disable=no-member
        assert Complex.parse('baz')[0] == ('baz', None)
        assert [(s.foo, s.bar) for s in Complex.parse('baz[a, b]')] == [('baz', 'a, b')]
        assert Complex.parse('baz[a, b], boo[c]') == (('baz', 'a, b'), ('boo', 'c'))
        with pytest.raises(error.Unexpected):
            Complex.parse(',baz[a, b]')
