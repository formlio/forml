"""
Simple operator unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.flow import task
from forml.flow.pipeline import topology
from forml.stdlib.operator import simple


class TestMapper:
    """Simple mapper unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def operator(actor: typing.Type[task.Actor]):
        """Operator fixture.
        """
        return simple.Mapper.operator(actor)()

    def test_compose(self, operator: topology.Operator):
        """Operator composition test.
        """
        operator.compose(topology.Origin())
