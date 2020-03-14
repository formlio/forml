"""
Project component tests.
"""
# pylint: disable=no-self-use
import importlib
import pathlib
import types
import typing

import pytest

from forml.project import component as compmod, importer


def test_setup():
    """Test the direct setup access.
    """
    compmod.setup(object())


class TestContext:
    """Context unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def name() -> str:
        """Module name fixture.
        """
        return 'foo'

    @staticmethod
    @pytest.fixture(scope='session')
    def module(name: str) -> types.ModuleType:
        """Module fixture.
        """
        return types.ModuleType(name)

    def test_context(self, name: str, module: types.ModuleType):
        """Testing the context manager.
        """
        with importer.context(module):
            assert importlib.import_module(name) == module


class TestVirtual:
    """Virtual component unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='session')
    def component() -> typing.Any:
        """Component fixture.
        """
        return object()

    @staticmethod
    @pytest.fixture(scope='session', params=(None, 'foo', 'bar.baz'))
    def package(request) -> str:
        """Package fixture.
        """
        return request.param

    def test_load(self, component: typing.Any, package: str):
        """Test loading of the virtual component.
        """
        assert compmod.load(compmod.Virtual(component, package=package).path) == component


def test_load():
    """Testing the top level component.load() function.
    """
    provided = compmod.load('component', pathlib.Path(__file__).parent)
    import component  # pylint: disable=import-outside-toplevel
    assert provided is component.INSTANCE
