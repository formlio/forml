"""
Simple operator unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml import flow
from forml.flow import segment, task
from forml.flow.operator import simple


class TestMapper:
    """Simple mapper unit tests.
    """
    @pytest.fixture(scope='function')
    def operator(self, actor: typing.Type[task.Actor]):
        """Operator fixture.
        """
        return simple.Mapper.operator(actor)()

    def test_compose(self, operator: flow.Operator):
        """Operator composition test.
        """
        operator.compose(segment.Origin())
