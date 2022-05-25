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
Dask runner.
"""
import functools
import importlib
import logging
import typing

import dask

from forml import flow, io, runtime
from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Runner(runtime.Runner, alias='dask'):
    """Dask based runner implementation."""

    class Dag(dict):
        """Dask DAG builder."""

        class Output(flow.Instruction):
            """Utility instruction for collecting multiple DAG leaves of which at most one is expected to return
            non-null value and passing that value through.
            """

            def execute(self, *leaves: typing.Any) -> typing.Any:
                """Instruction functionality.

                Args:
                    *leaves: Sequence of DAG leaves outputs.

                Return: Value of the one non-null output (if any).
                """

                def nonnull(value: typing.Any, element: typing.Any) -> typing.Any:
                    """Non-null reducer.

                    Args:
                        value: Carry over value.
                        element: Next element.

                    Returns:
                        Non-null value of the two (if any).
                    """
                    assert value is None or element is None, f'Multiple non-null outputs ({value}, {element})'
                    return element if value is None else value

                return functools.reduce(nonnull, leaves, None)

        def __init__(self, symbols: typing.Sequence[flow.Symbol]):
            tasks: dict[int, tuple[flow.Instruction, int]] = {id(i): (i, *(id(p) for p in a)) for i, a in symbols}
            assert len(tasks) == len(symbols), 'Duplicated symbols in DAG sequence'
            leaves = set(tasks).difference(p for _, *a in tasks.values() for p in a)
            assert leaves, 'Not acyclic'
            if len(leaves) > 1:
                LOGGER.debug(
                    'Dag output based on %d leaves: %s', len(leaves), ','.join(repr(tasks[n][0]) for n in leaves)
                )
                output = self.Output()
                self.output = id(output)
                tasks[self.output] = output, *leaves
            else:
                self.output = leaves.pop()
            super().__init__(tasks)

        def __repr__(self):
            return repr({k: (repr(i), *a) for k, (i, *a) in self.items()})

    SCHEDULER = 'multiprocessing'

    def __init__(
        self,
        instance: typing.Optional[asset.Instance] = None,
        feed: typing.Optional[io.Feed] = None,
        sink: typing.Optional[io.Sink] = None,
        scheduler: typing.Optional[str] = None,
    ):
        super().__init__(instance, feed, sink)
        self._scheduler: str = scheduler or self.SCHEDULER

    def _run(self, symbols: typing.Sequence[flow.Symbol]) -> None:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.
        """
        dag = self.Dag(symbols)
        LOGGER.debug('Dask DAG: %s', dag)
        importlib.import_module(f'{dask.__name__}.{self._scheduler}').get(dag, dag.output)
