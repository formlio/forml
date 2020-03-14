"""
ForML persistent unit tests.
"""
# pylint: disable=no-self-use
import pathlib
import tempfile
import typing

import pytest

from forml.runtime.asset import persistent
from forml.runtime.asset.persistent.registry import filesystem
from . import Registry


class TestRegistry(Registry):
    """Registry unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def constructor(tmp_path: pathlib.Path) -> typing.Callable[[], persistent.Registry]:
        return lambda: filesystem.Registry(tempfile.mkdtemp(dir=tmp_path))
