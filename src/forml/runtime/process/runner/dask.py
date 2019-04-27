"""
Dask runner.
"""
import functools
import logging
import typing

from dask import multiprocessing

from forml.runtime import code, process

LOGGER = logging.getLogger(__name__)


class Runner(process.Runner, key='dask'):
    """Dask based runner implementation.
    """
    class Dag(dict):
        """Dask DAG builder.
        """
        class Output(code.Instruction):
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

                    Returns: Non-null value of the two (if any).
                    """
                    assert value is None or element is None, f'Multiple non-null outputs ({value}, {element})'
                    return element if value is None else value
                return functools.reduce(nonnull, leaves, None)

        def __init__(self, symbols: typing.Sequence[code.Symbol]):
            tasks: typing.Dict[int, typing.Tuple[code.Instruction, int]] = {
                id(i): (i, *(id(p) for p in a)) for i, a in symbols}
            assert len(tasks) == len(symbols), 'Duplicated symbols in DAG sequence'
            leaves = set(tasks).difference(p for _, *a in tasks.values() for p in a)
            assert leaves, 'Not acyclic'
            if len(leaves) > 1:
                LOGGER.debug('Dag output based on %d leaves: %s', len(leaves), ','.join(str(l) for l in leaves))
                output = self.Output()
                self.output = id(output)
                tasks[self.output] = output, *leaves
            else:
                self.output = leaves.pop()
            super().__init__(tasks)

        def __str__(self):
            return str({k: (str(i), *a) for k, (i, *a) in self.items()})

    def _run(self, symbols: typing.Sequence[code.Symbol]) -> typing.Any:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.
        """
        dag = self.Dag(symbols)
        LOGGER.debug('Dask DAG: %s', dag)
        return multiprocessing.get(dag, dag.output)
