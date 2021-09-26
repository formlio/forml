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
import pytest

from forml.runtime.asset import directory, persistent
from forml.runtime.asset.directory import generation as genmod
from forml.runtime.asset.directory import lineage as lngmod
from forml.runtime.asset.directory import project as prjmod


class TestCache:
    """Directory cache tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def cache() -> directory.Cache:
        """Cache fixture."""
        instance = directory.Cache(persistent.Registry.open)
        instance.clear()
        return instance

    def test_cycle(
        self,
        cache: directory.Cache,
        registry: persistent.Registry,
        project_name: prjmod.Level.Key,
        populated_lineage: lngmod.Level.Key,
        valid_generation: genmod.Level.Key,
        tag: genmod.Tag,
    ):
        """Test the cache lifecycle."""
        assert cache.info.currsize == 0
        assert cache(registry, project_name, populated_lineage, valid_generation) == tag
        assert cache.info.misses == 1
        assert cache.info.currsize == 1
        assert cache(registry, project_name, populated_lineage, valid_generation) == tag
        assert cache.info.hits == 1
        cache.clear()
        assert cache.info.currsize == 0
