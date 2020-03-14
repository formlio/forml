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

            def __new__(cls, group, field):
                return super().__new__(cls, f'{group}:{field}')

        # pylint: disable=no-member
        assert Simple.parse('') == ()
        assert Simple.parse('bar')[0].foo == 'Simple:bar'
        assert [s.foo for s in Simple.parse('bar, baz', 'foo')] == ['foo:bar', 'foo:baz']
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

            def __new__(cls, group, field, params):
                return super().__new__(cls, f'{group}:{field}', params)

        # pylint: disable=no-member
        assert Complex.parse('baz')[0] == ('Complex:baz', None)
        assert [(s.foo, s.bar) for s in Complex.parse('baz[a, b]')] == [('Complex:baz', 'a, b')]
        assert Complex.parse('baz[a, b], boo[c]') == (('Complex:baz', 'a, b'), ('Complex:boo', 'c'))
        with pytest.raises(error.Unexpected):
            Complex.parse(',baz[a, b]')
