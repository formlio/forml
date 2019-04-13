"""
ForML asset access unit tests.
"""
import typing
# pylint: disable=no-self-use
import uuid

import pytest

from forml.runtime.asset import directory, access


class TestAssets:
    """Assets unit tests.
    """

    def test_tag(self, valid_assets: access.Assets, tag: directory.Generation.Tag):
        """Test default empty lineage generation retrieval.
        """
        assert valid_assets.tag is tag


class TestState:
    """State unit tests.
    """
    @staticmethod
    @pytest.fixture(scope='function')
    def state(valid_assets: access.Assets) -> access.State:
        """State fixture.
        """
        return valid_assets.state()

    def test_load(self, state: access.State, states: typing.Mapping[uuid.UUID, bytes]):
        """Test state loading.
        """
        for sid, value in states.items():
            assert state.load(sid) == value
