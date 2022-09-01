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

from forml import flow, runtime

if typing.TYPE_CHECKING:
    from forml import io
    from forml.io import asset

LOGGER = logging.getLogger(__name__)


class Runner(runtime.Runner, alias='dask'):
    """ForML runner implementation using the :doc:`Dask computing library <dask:index>` as the
    execution platform.

    Args:
        scheduler: Name of the chosen Dask scheduler. Supported options are:

                   * ``threaded``
                   * ``multiprocessing``

    The provider can be enabled using the following :ref:`platform configuration <platform-config>`:

    .. code-block:: toml
       :caption: config.toml

        [RUNNER.compute]
        provider = "dask"
        scheduler = "threaded"

    Important:
        Select the ``dask`` :ref:`extras to install <install-extras>` ForML together with the Dask
        support.
    """

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

        def __init__(self, symbols: typing.Collection[flow.Symbol]):
            tasks: dict[int, tuple[flow.Instruction, int]] = {id(i): (i, *(id(p) for p in a)) for i, a in symbols}
            assert len(tasks) == len(symbols), 'Duplicated symbols in DAG sequence'
            leaves = set(tasks).difference(p for _, *a in tasks.values() for p in a)
            assert leaves, 'Not acyclic'
            if (leaves_len := len(leaves)) > 1:
                LOGGER.debug(
                    'Dag output based on %d leaves: %s', leaves_len, ','.join(repr(tasks[n][0]) for n in leaves)
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
        instance: typing.Optional['asset.Instance'] = None,
        feed: typing.Optional['io.Feed'] = None,
        sink: typing.Optional['io.Sink'] = None,
        scheduler: typing.Optional[str] = None,
    ):
        super().__init__(instance, feed, sink, scheduler=scheduler)

    @classmethod
    def run(cls, symbols: typing.Collection[flow.Symbol], **kwargs) -> None:
        dag = cls.Dag(symbols)
        LOGGER.debug('Dask DAG: %s', dag)
        scheduler = kwargs.get('scheduler') or cls.SCHEDULER
        importlib.import_module(f'{dask.__name__}.{scheduler}').get(dag, dag.output)
