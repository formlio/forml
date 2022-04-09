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
Service facility worker tests.
"""
# pylint: disable=no-self-use
import multiprocessing

import pytest

from forml import io
from forml.io import asset, layout
from forml.runtime.facility._service import prediction


class TestPool:
    """Worker pool unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def tasks() -> multiprocessing.Queue:
        """Tasks queue fixture."""
        with multiprocessing.Manager() as manager:
            yield manager.Queue()

    @staticmethod
    @pytest.fixture(scope='function')
    def results() -> multiprocessing.Queue:
        """Results queue fixture."""
        with multiprocessing.Manager() as manager:
            yield manager.Queue()

    @staticmethod
    @pytest.fixture(scope='function')
    def pool(
        valid_instance: asset.Instance,
        feed_instance: io.Feed,
        tasks: multiprocessing.Queue,
        results: multiprocessing.Queue,
    ) -> prediction.Pool:
        """Pool fixture."""
        with multiprocessing.Manager() as manager:
            yield prediction.Pool(valid_instance, feed_instance, tasks, results, stopped=manager.Event(), processes=3)

    @staticmethod
    @pytest.fixture(scope='session')
    def input_task(testset_entry: layout.Entry) -> prediction.Task:
        """Tasks fixture."""
        return prediction.Task(1, testset_entry)

    def test_work(
        self,
        pool: prediction.Pool,
        tasks: multiprocessing.Queue,
        results: multiprocessing.Queue,
        input_task: prediction.Task,
        generation_prediction: layout.Array,
    ):
        """Pool work unit testing."""
        assert tasks.empty()
        assert results.empty()
        assert not pool.is_alive()
        pool.start()
        assert pool.is_alive()
        tasks.put(input_task)
        result: prediction.Result = results.get()
        assert result.id == input_task.id
        assert tuple(result.outcome.data) == generation_prediction
        pool.stop()
        assert not pool.is_alive()


class TestExecutor:
    """Executor unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def executor(
        valid_instance: asset.Instance,
        feed_instance: io.Feed,
    ) -> prediction.Executor:
        """Executor fixture."""
        return prediction.Executor(valid_instance, feed_instance, processes=3)

    def test_apply(
        self, executor: prediction.Executor, testset_entry: layout.Entry, generation_prediction: layout.Array
    ):
        """Apply unit test."""
        with pytest.raises(RuntimeError, match='Executor not running'):
            executor.apply(testset_entry)
        executor.start()
        outcome = executor.apply(testset_entry)
        assert tuple(outcome.result().data) == generation_prediction
        executor.stop()
