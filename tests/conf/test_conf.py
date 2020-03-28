"""
ForML config unit tests.
"""
# pylint: disable=protected-access,no-self-use
import pathlib
import types


def test_exists(cfg_file: pathlib.Path):
    """Test the config file exists.
    """
    assert cfg_file.is_file()


def test_src(conf: types.ModuleType, cfg_file: pathlib.Path):
    """Test the registry config field.
    """
    assert set(conf.SRC) == {str(cfg_file)}


def test_get(conf: types.ModuleType):
    """Test the get value matches the test config.ini
    """
    assert conf.get('foobar') == 'baz'
