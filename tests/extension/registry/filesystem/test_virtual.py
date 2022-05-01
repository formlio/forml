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
ForML virtual registry unit tests.
"""
# pylint: disable=no-self-use
import typing

import pytest

from forml.extension.registry.filesystem import virtual
from forml.io import asset

from .. import Registry


class TestRegistry(Registry):
    """Registry unit tests."""

    @staticmethod
    @pytest.fixture(scope='session')
    def constructor() -> typing.Callable[[], asset.Registry]:
        return virtual.Registry
