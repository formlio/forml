"""
Dask runner.
"""

import typing

from dask import threaded

from forml import etl
from forml.runtime import interpreter, assembly
from forml.runtime.asset import access


class Runner(interpreter.Runner):
    """Dask based runner implementation.
    """
    def __init__(self, engine: etl.Engine[etl.OrdinalT], assets: access.Assets):
        super().__init__(engine, assets)

    def _run(self, symbols: typing.Sequence[assembly.Symbol]) -> None:
        dag = dict(symbols)
        threaded.get(dag, )