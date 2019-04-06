"""
ForML runtime task graph compilation.
"""

import logging
import typing

from forml import etl, flow
from forml.flow import segment
from forml.flow.graph import view
from forml.runtime import assembly
from forml.runtime.assembly import symbol
from forml.runtime.asset import access, persistent

LOGGER = logging.getLogger(__name__)


def generate(path: view.Path, assets: access.State) -> typing.Sequence[assembly.Symbol]:
    """Generate the symbol code based on given flow path.

    Args:
        path: Flow path to generate the symbols for.
        assets: Runtime assets dependencies.

    Returns: Sequence of symbol code.
    """
    table = symbol.Table(assets)
    path.accept(table)
    return tuple(table)


class Goal:
    """Goal implements the high-level steps of assembling the task graph based on the selected mode combining with
    particular ETL engine and possibly any persistence steps.
    """
    def __init__(self, engine: etl.Engine, assets: access.Assets):
        self._engine: etl.Engine = engine
        self._assets: access.Assets = assets

    @classmethod
    def load(cls, engine: etl.Engine, registry: persistent.Registry, project: str, lineage: typing.Optional[int] = None,
             generation: typing.Optional[int] = None) -> 'Goal':
        """Create the linker instance based on project loaded from a registry.

        Args:
            engine: ETL engine to use.

        Returns: Linker instance.
        """
        return cls(engine, access.Assets(registry, project, lineage, generation))

    def _assemble(self, lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT],
                  *blocks: segment.Track) -> flow.Pipeline:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns: Assembled flow pipeline.
        """
        return flow.Pipeline.compose(self._engine.load(self._assets.project.source, lower, upper), *blocks)

    def training(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[assembly.Symbol]:
        """Return the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Training code.
        """
        path = self._assemble(lower or self._assets.tag.training.ordinal, upper,
                              self._assets.project.pipeline.expand()).train
        return generate(path, self._assets.state(self._assets.tag.training.trigger()))

    def applying(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[assembly.Symbol]:
        """Return the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        path = self._assemble(lower, upper, self._assets.project.pipeline.expand()).apply
        return generate(path, self._assets.state())
