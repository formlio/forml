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
Runtime service facility worker.
"""
import logging
import multiprocessing
import os
import queue
import threading
import typing
from concurrent import futures
from multiprocessing import context

from forml import io
from forml.io import asset, dsl, layout
from forml.provider.runner import pyfunc

LOGGER = logging.getLogger(__name__)


class Result(typing.NamedTuple):
    """Result tuple."""

    id: int  # pylint: disable=invalid-name
    """Task id."""
    outcome: typing.Optional[layout.Outcome]
    """Task outcome if successful."""
    exception: typing.Optional[BaseException]
    """Task exception if failed."""


class Task(typing.NamedTuple):
    """Task tuple."""

    id: int  # pylint: disable=invalid-name
    """Task id."""
    entry: layout.Entry
    """Task input."""

    def success(self, outcome: layout.Outcome) -> 'Result':
        """Create a descriptor representing successful task result.

        Args:
            outcome: Actual result value.

        Returns:
            Result instance.
        """
        return Result(self.id, outcome, None)

    def failure(self, exception: BaseException) -> 'Result':
        """Create a descriptor representing failed task result.

        Args:
            exception: Actual result exception.

        Returns:
            Result instance.
        """
        return Result(self.id, None, exception)


class Pool(context.SpawnProcess):
    """Pool of worker processes.

    Upon starting, this spawns a subprocess with the Pyfunc runner instance, which loads all states so that all the
    forked workers share just one (read-only) copy of the memory.
    """

    class Sink(io.Sink):
        """Dummy sink that only returns combo of the schema and the payload."""

        class Consumer:
            """Primitive consumer just passing the data."""

            def __init__(self, schema: typing.Optional[dsl.Source.Schema]):
                self._schema: typing.Optional[dsl.Source.Schema] = schema

            def __call__(self, data: layout.RowMajor) -> layout.Outcome:
                if not self._schema:
                    LOGGER.warning('Inferring unknown output schema')
                    self._schema = dsl.Schema.from_record(data[0])
                return layout.Outcome(self._schema, data)

        @classmethod
        def consumer(cls, schema: typing.Optional[dsl.Source.Schema], **kwargs: typing.Any) -> io.Consumer:
            return cls.Consumer(schema)

    class Worker(context.ForkProcess):
        """Pool worker implementation."""

        def __init__(
            self,
            runner: pyfunc.Runner,
            tasks: multiprocessing.Queue,
            results: multiprocessing.Queue,
            stopped: multiprocessing.Event,
            name: typing.Optional[str] = None,
        ):
            super().__init__(daemon=True, name=(name or 'worker'))
            self._runner: pyfunc.Runner = runner
            self._tasks: multiprocessing.Queue = tasks
            self._results: multiprocessing.Queue = results
            self._stopped: multiprocessing.Event = stopped
            self.start()

        def run(self) -> None:
            """Worker loop."""
            LOGGER.debug('Worker loop %s starting', self.name)
            while not self._stopped.is_set():
                try:
                    task: Task = self._tasks.get(timeout=1)
                except queue.Empty:
                    continue
                try:
                    self._results.put_nowait(task.success(self._runner.call(task.entry)))
                except Exception as err:
                    self._results.put_nowait(task.failure(err))
                    self._stopped.set()
                    raise err
            LOGGER.debug('Worker loop %s quiting', self.name)

    def __init__(
        self,
        instance: asset.Instance,
        feed: io.Feed,
        tasks: multiprocessing.Queue,
        results: multiprocessing.Queue,
        stopped: multiprocessing.Event,
        processes: typing.Optional[int] = None,
        name: typing.Optional[str] = None,
    ):
        super().__init__(name=(name or 'pool'))
        self._instance: asset.Instance = instance
        self._feed: io.Feed = feed
        self._tasks: multiprocessing.Queue = tasks
        self._results: multiprocessing.Queue = results
        self._stopped: multiprocessing.Event = stopped
        self._processes: int = processes or os.cpu_count()

    def run(self) -> None:
        """Pool loop."""
        LOGGER.debug('Worker pool %s starting', self.name)
        runner: pyfunc.Runner = pyfunc.Runner(self._instance, self._feed, self.Sink())
        pool: list[context.ForkProcess] = [
            self.Worker(runner, self._tasks, self._results, self._stopped, name=f'{self.name}:{i}')
            for i in range(self._processes)
        ]
        while all(w.is_alive() for w in pool):
            if self._stopped.wait(1):
                break
        else:
            self._stopped.set()
        for worker in pool:
            worker.join()
        LOGGER.debug('Worker pool %s quiting', self.name)

    def stop(self) -> None:
        """Stop the pool."""
        self._stopped.set()
        self.join()


class Executor(threading.Thread):
    """Asynchronous worker frontend dispatching the worker pool."""

    def __init__(
        self,
        instance: asset.Instance,
        feed: io.Feed,
        processes: typing.Optional[int] = None,
        name: typing.Optional[str] = None,
    ):
        super().__init__(daemon=True, name=(name or 'executor'))
        self._manager: multiprocessing.Manager = multiprocessing.Manager()
        self._stopped: multiprocessing.Event = self._manager.Event()
        self._tasks: multiprocessing.Queue = self._manager.Queue()
        self._results: multiprocessing.Queue = self._manager.Queue()
        self._pool: Pool = Pool(instance, feed, self._tasks, self._results, self._stopped, processes)
        self._pending: dict[int, futures.Future[layout.Outcome]] = {}
        self._index: int = 0

    def run(self) -> None:
        """Executor loop."""
        LOGGER.debug('Executor loop %s starting', self.name)
        while self._pool.is_alive():
            if self._stopped.is_set():
                break
            try:
                result = self._results.get(timeout=1)
            except queue.Empty:
                continue
            if result.exception:
                self._pending[result.id].set_exception(result.exception)
            else:
                self._pending[result.id].set_result(result.outcome)
            del self._pending[result.id]
        else:
            self._stopped.set()
        LOGGER.debug('Executor loop %s quiting', self.name)

    def apply(self, entry: layout.Entry) -> futures.Future[layout.Outcome]:
        """Submit the given entry data to the processing pool and return a future result instance.

        Args:
            entry: Input data.

        Returns:
            Future result instance.
        """
        if not self.is_alive():
            raise RuntimeError('Executor not running')
        outcome = futures.Future()
        self._pending[self._index] = outcome
        self._tasks.put(Task(self._index, entry))
        self._index += 1
        return outcome

    def start(self) -> None:
        """Start the executor."""
        self._stopped.clear()
        self._pool.start()
        super().start()

    def stop(self) -> None:
        """Stop the executor."""
        self._stopped.set()
        self._pool.join()
        self.join()
        self._manager.shutdown()
