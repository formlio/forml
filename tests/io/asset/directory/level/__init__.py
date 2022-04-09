# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
ForML asset directory unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.io import asset


class Level:
    """Common level functionality."""

    def test_default(
        self,
        parent: typing.Callable[[typing.Optional[asset.Level.Key]], asset.Level],
        last_level: asset.Level.Key,
    ):
        """Test default level retrieval."""
        assert parent(None).key == last_level

    def test_explicit(
        self,
        parent: typing.Callable[[typing.Optional[asset.Level.Key]], asset.Level],
        valid_level: asset.Level.Key,
        invalid_level: asset.Level.Key,
    ):
        """Test explicit level retrieval."""
        assert parent(valid_level).key == valid_level
        with pytest.raises(asset.Level.Invalid):
            assert parent(invalid_level).key
