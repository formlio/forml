"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.runtime.asset import directory


class Level:
    """Common level functionality.
    """
    def test_default(self, parent: typing.Callable[[typing.Optional[directory.KeyT]], directory.Level],
                     last_level: directory.KeyT):
        """Test default level retrieval.
        """
        assert parent(None).key == last_level

    def test_explicit(self, parent: typing.Callable[[typing.Optional[directory.KeyT]], directory.Level],
                      valid_level: directory.KeyT, invalid_level: directory.KeyT):
        """Test explicit level retrieval.
        """
        assert parent(valid_level).key == valid_level
        with pytest.raises(directory.Level.Invalid):
            assert parent(invalid_level).key
