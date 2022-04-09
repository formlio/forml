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

from forml.io import asset
from forml.io.asset import _directory


class TestCache:
    """Directory cache tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def cache() -> _directory.Cache:
        """Cache fixture."""
        instance = _directory.Cache(asset.Registry.open)
        instance.clear()
        return instance

    def test_cycle(
        self,
        cache: _directory.Cache,
        registry: asset.Registry,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
        generation_tag: asset.Tag,
    ):
        """Test the cache lifecycle."""
        assert cache.info.currsize == 0
        assert cache(registry, project_name, project_release, valid_generation) == generation_tag
        assert cache.info.misses == 1
        assert cache.info.currsize == 1
        assert cache(registry, project_name, project_release, valid_generation) == generation_tag
        assert cache.info.hits == 1
        cache.clear()
        assert cache.info.currsize == 0
