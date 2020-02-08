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
    def assets(valid_assets: access.Assets, nodes: typing.Sequence[uuid.UUID]) -> typing.ContextManager[access.State]:
        """State fixture.
        """
        return valid_assets.state(nodes)

    def test_load(self, assets: typing.ContextManager[access.State], nodes: typing.Sequence[uuid.UUID],
                  states: typing.Mapping[uuid.UUID, bytes]):
        """Test state loading.
        """
        with assets as accessor:
            for node, value in zip(nodes, states.values()):
                assert accessor.load(node) == value
