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
import typing

import pytest

import forml
from forml import project as prj
from forml.io import asset

from . import Level


class TestVersion:
    """Release version unit tests."""

    def test_parse(self):
        """Parsing test."""
        ver = asset.Release.Key('0.1.dev2')
        asset.Release.Key(ver)
        asset.Release.Key(forml.__version__)
        with pytest.raises(asset.Release.Key.Invalid):
            asset.Release.Key('foobar')


class TestLevel(Level):
    """Release unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def parent(
        directory: asset.Directory, project_name: asset.Project.Key
    ) -> typing.Callable[[typing.Optional[asset.Release.Key]], asset.Release]:
        """Parent fixture."""
        return lambda release: directory.get(project_name).get(release)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(project_release: asset.Release.Key) -> asset.Release.Key:
        """Level fixture."""
        return project_release

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_release: asset.Release.Key) -> asset.Release.Key:
        """Level fixture."""
        return last_release

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_release: asset.Release.Key) -> asset.Release.Key:
        """Level fixture."""
        return asset.Release.Key(f'{last_release.release[0] + 1}')

    def test_empty(
        self,
        parent: typing.Callable[[typing.Optional[asset.Release.Key]], asset.Release],
        empty_release: asset.Release.Key,
    ):
        """Test default empty release generation retrieval."""
        generation = parent(empty_release).get()
        with pytest.raises(asset.Level.Listing.Empty):
            _ = generation.key
        assert not generation.tag.states

    def test_artifact(
        self, directory: asset.Directory, project_name: asset.Project.Key, invalid_level: asset.Release.Key
    ):
        """Registry take unit test."""
        with pytest.raises(asset.Level.Invalid):
            _ = directory.get(project_name).get(invalid_level).artifact

    def test_put(self, directory: asset.Directory, project_name: asset.Project.Key, project_package: prj.Package):
        """Registry put unit test."""
        with pytest.raises(asset.Level.Invalid):  # release already exists
            directory.get(project_name).put(project_package)
