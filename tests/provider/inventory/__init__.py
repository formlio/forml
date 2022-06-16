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
import abc
import pickle
import typing

import pytest

from forml import project
from forml.io import asset


class Inventory(metaclass=abc.ABCMeta):
    """Base class for inventory unit tests."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def constructor() -> typing.Callable[[], asset.Inventory]:
        """Inventory fixture."""

    @staticmethod
    @pytest.fixture(scope='function')
    def empty(constructor: typing.Callable[[], asset.Inventory]) -> asset.Inventory:
        """Empty inventory fixture."""
        return constructor()

    @staticmethod
    @pytest.fixture(scope='function')
    def populated(
        constructor: typing.Callable[[], asset.Inventory], descriptor_handle: project.Descriptor.Handle
    ) -> asset.Inventory:
        """Populated inventory fixture."""
        inventory = constructor()
        inventory.put(descriptor_handle)
        return inventory

    def test_list(self, empty: asset.Inventory, populated: asset.Inventory, application: str):
        """Inventory listing unit test."""
        assert not any(empty.list())
        assert application in populated.list()

    def test_get(self, populated: asset.Inventory, descriptor: project.Descriptor):
        """Inventory get unit test."""
        assert populated.get(descriptor.name) == descriptor

    def test_put(self, populated: asset.Inventory, descriptor_handle: project.Descriptor.Handle):
        """Inventory put unit test."""
        populated.put(descriptor_handle)

    def test_serializable(self, populated: asset.Inventory):
        """Test inventory serializability."""
        assert pickle.loads(pickle.dumps(populated)).list() == populated.list()
