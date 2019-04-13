"""
Dask runner.
"""
import logging
import typing

from dask import threaded

from forml import etl
from forml.runtime import interpreter, assembly
from forml.runtime.asset import access


LOGGER = logging.getLogger(__name__)


class Runner(interpreter.Runner):
    """Dask based runner implementation.
    """
    def __init__(self, engine: etl.Engine[etl.OrdinalT], assets: access.Assets):
        super().__init__(engine, assets)

    def _run(self, symbols: typing.Sequence[assembly.Symbol]) -> None:
        dag = {i: (i, *a) for i, a in symbols}
        LOGGER.debug('Dask DAG: %s', {id(k): tuple(id(i) for i in t) for k, t in dag.items()})
        threaded.get(dag, symbols[-1].instruction)
