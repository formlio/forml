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
ForML persistent inventory unit tests.
"""
# pylint: disable=no-self-use
import pathlib
import tempfile
import typing

import pytest

from forml.io import asset
from forml.lib.inventory import posix

from . import Inventory


class TestInventory(Inventory):
    """Inventory unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def constructor(tmp_path: pathlib.Path) -> typing.Callable[[], asset.Inventory]:
        return lambda: posix.Inventory(tempfile.mkdtemp(dir=tmp_path))
