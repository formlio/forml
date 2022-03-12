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
from forml.io import layout
from forml.runtime import asset
from forml.runtime.facility._service import worker


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
    ) -> worker.Pool:
        """Pool fixture."""
        with multiprocessing.Manager() as manager:
            yield worker.Pool(valid_instance, feed_instance, tasks, results, stopped=manager.Event(), processes=3)

    @staticmethod
    @pytest.fixture(scope='session')
    def input_task(input_entry: layout.Entry) -> worker.Task:
        """Tasks fixture."""
        return worker.Task(1, input_entry)

    def test_work(
        self,
        pool: worker.Pool,
        tasks: multiprocessing.Queue,
        results: multiprocessing.Queue,
        input_task: worker.Task,
        generation_prediction: layout.Array,
    ):
        """Pool work unit testing."""
        assert tasks.empty()
        assert results.empty()
        assert not pool.is_alive()
        pool.start()
        assert pool.is_alive()
        tasks.put(input_task)
        result: worker.Result = results.get()
        assert result.id == input_task.id
        assert tuple(result.outcome[1]) == generation_prediction
        pool.stop()
        assert not pool.is_alive()


class TestExecutor:
    """Executor unit tests."""

    @staticmethod
    @pytest.fixture(scope='function')
    def executor(
        valid_instance: asset.Instance,
        feed_instance: io.Feed,
    ) -> worker.Executor:
        """Executor fixture."""
        return worker.Executor(valid_instance, feed_instance, processes=3)

    def test_apply(self, executor: worker.Executor, input_entry: layout.Entry, generation_prediction: layout.Array):
        """Apply unit test."""
        with pytest.raises(RuntimeError, match='Executor not running'):
            executor.apply(input_entry)
        executor.start()
        outcome = executor.apply(input_entry)
        assert tuple(outcome.result()[1]) == generation_prediction
        executor.stop()
