"""
ForML config unit tests.
"""
# pylint: disable=protected-access,no-self-use

import pytest


class TestSection:
    """SectionMeta unit tests.
    """
    def test_simple(self, conf):
        """Test with default pattern.
        """
        class Simple(metaclass=conf.SectionMeta):  # pylint: disable=undefined-variable
            """CSV specified field list.
            """
            FIELDS = 'foo'

            def __new__(cls, group, field):
                return super().__new__(cls, f'{group}:{field}')

        # pylint: disable=no-member
        assert Simple.parse('') == ()
        assert Simple.parse('bar')[0].foo == 'Simple:bar'
        assert [s.foo for s in Simple.parse('bar, baz', 'foo')] == ['foo:bar', 'foo:baz']
        with pytest.raises(conf.Error):
            Simple.parse(',bar')
        with pytest.raises(conf.Error):
            Simple.parse('bar, bar')

    def test_complex(self, conf):
        """Test for the conf.SectionMeta metaclass with default pattern.
        """
        class Complex(metaclass=conf.SectionMeta):  # pylint: disable=undefined-variable
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
        with pytest.raises(conf.Error):
            Complex.parse(',baz[a, b]')


class TestConf:
    """Conf unit tests.
    """
    def test_registry(self, conf):
        """Test the registry config field.
        """
        cfg = conf.REGISTRY
        assert cfg.key == 'virtual'

    def test_engine(self, conf):
        """Test the engine config field.
        """
        cfg = conf.ENGINE
        assert cfg.key == 'devio'

    def test_runner(self, conf):
        """Test the runner config field.
        """
        cfg = conf.RUNNER
        assert cfg.key == 'dask'
