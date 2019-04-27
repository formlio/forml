"""
Dask runner.
"""
import collections
import functools
import logging
import typing

from dask import threaded

from forml.runtime import code, process

LOGGER = logging.getLogger(__name__)


class Interpreter(process.Runner, key='dask'):
    """Dask based runner implementation.
    """
    class Dag(collections.Mapping):
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
                    assert not (value and element), f'Multiple non-null outputs ({value}, {element})'
                    return value if value else element
                return functools.reduce(nonnull, leaves, None)

        def __init__(self, symbols: typing.Sequence[code.Symbol]):
            self._instructions: typing.Dict[code.Instruction, typing.Sequence[code.Instruction]] = dict(symbols)
            assert len(self._instructions) == len(symbols), 'Duplicated symbols in DAG sequence'
            inputs = {i for a in self._instructions.values() for i in a}
            leaves = tuple(i for i in self._instructions if i not in inputs)
            assert leaves, 'Not acyclic'
            if len(leaves) > 1:
                LOGGER.debug('Dag output based on %d leaves: %s', len(leaves), ','.join(str(l) for l in leaves))
                self.output = self.Output()
                self._instructions[self.output] = leaves
            else:
                self.output = leaves[0]

        def __getitem__(self, instruction: code.Instruction) -> typing.Sequence[code.Instruction]:
            return (instruction, *self._instructions[instruction])

        def __len__(self) -> int:
            return len(self._instructions)

        def __iter__(self) -> typing.Iterator[code.Instruction]:
            return iter(self._instructions)

        def __str__(self):
            return str({id(k): tuple(id(i) for i in self[k]) for k in self})

    def _run(self, symbols: typing.Sequence[code.Symbol]) -> None:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.
        """
        dag = self.Dag(symbols)
        LOGGER.debug('Dask DAG: %s', dag)
        print(threaded.get(dag, dag.output))
