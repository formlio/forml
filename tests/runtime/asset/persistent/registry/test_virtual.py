"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.runtime.asset import persistent
from forml.runtime.asset.persistent.registry import virtual
from . import Registry


class TestRegistry(Registry):
    """Registry unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def constructor() -> typing.Callable[[], persistent.Registry]:
        return virtual.Registry
