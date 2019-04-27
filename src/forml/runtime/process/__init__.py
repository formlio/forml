"""
Runtime process layer.
"""
import abc
import typing

from forml import provider, etl
from forml.flow import pipeline
from forml.runtime import code
from forml.runtime.asset import access
from forml.runtime.code import compiler


class Runner(provider.Interface):
    """Abstract base runtime class to be extended by particular runtime implementations.
    """
    def __init__(self, engine: etl.Engine, assets: access.Assets):
        self._engine: etl.Engine = engine
        self._assets: access.Assets = assets

    def _build(self, lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT],
               *blocks: pipeline.Segment) -> pipeline.Composition:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns: Assembled flow pipeline.
        """
        return pipeline.Composition(self._engine.load(self._assets.project.source, lower, upper), *blocks)

    def train(self, lower: typing.Optional[etl.OrdinalT] = None,
              upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Return the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Training code.
        """
        path = self._build(lower or self._assets.tag.training.ordinal, upper,
                           self._assets.project.pipeline.expand()).train
        self._run(compiler.generate(path, self._assets.state(self._assets.tag.training.trigger())))

    def apply(self, lower: typing.Optional[etl.OrdinalT] = None,
              upper: typing.Optional[etl.OrdinalT] = None) -> typing.Any:
        """Return the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        path = self._build(lower, upper, self._assets.project.pipeline.expand()).apply
        return self._run(compiler.generate(path, self._assets.state()))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[code.Symbol]) -> typing.Any:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.
        """
