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
