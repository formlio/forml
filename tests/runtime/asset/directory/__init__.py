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
    def test_default(self, parent: typing.Callable[[typing.Optional[directory.Level.Key]], directory.Level],
                     last_level: directory.Level.Key):
        """Test default level retrieval.
        """
        assert parent(None).key == last_level

    def test_explicit(self, parent: typing.Callable[[typing.Optional[directory.Level.Key]], directory.Level],
                      valid_level: directory.Level.Key, invalid_level: directory.Level.Key):
        """Test explicit level retrieval.
        """
        assert parent(valid_level).key == valid_level
        with pytest.raises(directory.Level.Invalid):
            assert parent(invalid_level).key
