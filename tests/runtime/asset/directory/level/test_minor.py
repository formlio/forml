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
import uuid

import pytest

from forml.runtime import asset

from . import Level


class TestLevel(Level):
    """Generation unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def parent(
        directory: asset.Directory, project_name: str, project_lineage: asset.Lineage.Key
    ) -> typing.Callable[[typing.Optional[asset.Generation.Key]], asset.Generation]:
        """Parent fixture."""
        return lambda generation: directory.get(project_name).get(project_lineage).get(generation)

    @staticmethod
    @pytest.fixture(scope='session')
    def valid_level(valid_generation: asset.Generation.Key) -> asset.Generation.Key:
        """Level fixture."""
        return valid_generation

    @staticmethod
    @pytest.fixture(scope='session')
    def last_level(last_generation: asset.Generation.Key) -> asset.Generation.Key:
        """Level fixture."""
        return last_generation

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_level(last_generation: asset.Generation.Key) -> asset.Generation.Key:
        """Level fixture."""
        return last_generation + 1

    @staticmethod
    @pytest.fixture(scope='session')
    def invalid_lineage(last_lineage: asset.Lineage.Key) -> asset.Lineage.Key:
        """Level fixture."""
        return asset.Lineage.Key(f'{last_lineage.release[0] + 1}')

    def test_tag(
        self,
        directory: asset.Directory,
        project_name: asset.Project.Key,
        project_lineage: asset.Lineage.Key,
        empty_lineage: asset.Lineage.Key,
        valid_generation: asset.Generation.Key,
        generation_tag: asset.Tag,
    ):
        """Registry checkout unit test."""
        project = directory.get(project_name)
        with pytest.raises(asset.Level.Invalid):
            _ = project.get(empty_lineage).get(valid_generation).tag
        assert project.get(project_lineage).get(valid_generation).tag == generation_tag
        assert project.get(empty_lineage).get(None).tag == asset.Tag()

    def test_read(
        self,
        directory: asset.Directory,
        project_name: asset.Project.Key,
        project_lineage: asset.Lineage.Key,
        invalid_lineage: asset.Lineage.Key,
        valid_generation: asset.Generation.Key,
        generation_states: typing.Mapping[uuid.UUID, bytes],
    ):
        """Registry load unit test."""
        project = directory.get(project_name)
        with pytest.raises(asset.Level.Invalid):
            project.get(invalid_lineage).get(None).get(None)
        with pytest.raises(asset.Level.Invalid):
            project.get(project_lineage).get(valid_generation).get(None)
        for sid, value in generation_states.items():
            assert project.get(project_lineage).get(valid_generation).get(sid) == value


class TestTag:
    """Generation tag unit tests."""

    def test_replace(self, generation_tag: asset.Tag):
        """Test replace strategies."""
        assert generation_tag.replace(states=(1, 2, 3)).states == (1, 2, 3)
        with pytest.raises(ValueError):
            generation_tag.replace(invalid=123)
        with pytest.raises(ValueError):
            generation_tag.replace(training=123)
        with pytest.raises(ValueError):
            generation_tag.replace(tuning=123)
        assert generation_tag.training.replace(ordinal=123).training.ordinal == 123
        assert generation_tag.tuning.replace(score=123).tuning.score == 123
        with pytest.raises(TypeError):
            generation_tag.training.replace(invalid=123)

    def test_trigger(self, generation_tag: asset.Tag):
        """Test triggering."""
        trained = generation_tag.training.trigger()
        assert trained.training.timestamp > generation_tag.training.timestamp
        assert trained.tuning == generation_tag.tuning
        tuned = generation_tag.tuning.trigger()
        assert tuned.tuning.timestamp > generation_tag.tuning.timestamp
        assert tuned.training == generation_tag.training

    def test_bool(self):
        """Test the boolean mode values."""
        empty = asset.Tag()
        assert not empty.training
        assert not empty.tuning
        assert empty.training.trigger().training
        assert empty.tuning.trigger().tuning

    def test_dumpload(self, generation_tag: asset.Tag):
        """Test tag serialization."""
        assert asset.Tag.loads(generation_tag.dumps()) == generation_tag
