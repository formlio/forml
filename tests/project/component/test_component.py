"""
Project component tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.project import component as compload


def test_setup():
    """Test the direct setup access.
    """
    compload.setup(object())


class TestContext:
    """Context unit tests.
    """
    def test_import(self):
        """Testing the contextual import.
        """
        def handler(instance: typing.Any) -> None:
            """Context handler storing the instance in the enclosing scope.

            Args:
                instance: provided instance to be stored.
            """
            nonlocal provided
            provided = instance

        provided = None
        with compload.Context(handler):
            import component  # component.py in this test directory
            assert provided is component.INSTANCE

        from forml.project import component as compreload  # pylint: disable=reimported
        compreload.setup(object())
        assert provided is component.INSTANCE


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
        assert compload.load(compload.Virtual(component, package=package).path) == component
