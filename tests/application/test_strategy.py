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
Strategy unit tests.
"""
import abc
import typing

import pytest

from forml import application, runtime
from forml.io import asset


class Strategy(abc.ABC):
    """Base class for strategy tests."""

    @staticmethod
    @abc.abstractmethod
    @pytest.fixture(scope='function')
    def strategy() -> application.Selector:
        """Strategy fixture."""

    @staticmethod
    @pytest.fixture(scope='function')
    def instance(valid_instance: asset.Instance) -> asset.Instance:
        """Instance fixture."""
        return valid_instance

    @staticmethod
    @pytest.fixture(scope='function')
    def context() -> typing.Any:
        """Context fixture."""
        return None

    @staticmethod
    @pytest.fixture(scope='function')
    def stats() -> runtime.Stats:
        """Stats fixture."""
        return runtime.Stats()

    def test_select(
        self,
        strategy: application.Selector,
        instance: asset.Instance,
        directory: asset.Directory,
        context: typing.Any,
        stats: runtime.Stats,
    ):
        """Strategy select test."""
        assert strategy.select(directory, context, stats) == instance


class TestExplicit(Strategy):
    """Explicit strategy unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def strategy(
        project_name: asset.Project.Key, project_release: asset.Release.Key, valid_generation: asset.Generation.Key
    ) -> application.Selector:
        return application.Explicit(project_name, project_release, valid_generation)


class TestLatest(Strategy):
    """Latest strategy unit tests."""

    @staticmethod
    @pytest.fixture(scope='function', params=[False, True])
    def strategy(
        request: pytest.FixtureRequest, project_name: asset.Project.Key, project_release: asset.Release.Key
    ) -> application.Selector:
        release = project_release if request.param else None
        return application.Latest(project_name, release)


class TestABTest(Strategy):
    """ABTEst strategy unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def strategy(
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
        empty_release: asset.Release.Key,
    ) -> application.Selector:
        return application.ABTest.compare(project_name, project_release, valid_generation).against(
            release=empty_release, generation=valid_generation
        )

    def test_invalid(
        self,
        project_name: asset.Project.Key,
        project_release: asset.Release.Key,
        valid_generation: asset.Generation.Key,
    ):
        """Test invalid conditions."""
        with pytest.raises(ValueError, match='Positive target required'):
            application.ABTest.compare(project_name, project_release, valid_generation, 0)
        with pytest.raises(ValueError, match='Exclusive variants required'):
            application.ABTest.compare(project_name, project_release, valid_generation).against(
                project=project_name, release=project_release, generation=valid_generation
            )
