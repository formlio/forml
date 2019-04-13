"""
Runtime interpreter.
"""
import abc
import typing

from forml import etl, flow
from forml.flow import segment
from forml.runtime import assembly
from forml.runtime.assembly import compiler
from forml.runtime.asset import access


class Runner(typing.Generic[etl.OrdinalT], metaclass=abc.ABCMeta):
    """Abstract base runtime class to be extended by particular runtime implementations.
    """
    def __init__(self, engine: etl.Engine[etl.OrdinalT], assets: access.Assets):
        self._engine: etl.Engine[etl.OrdinalT] = engine
        self._assets: access.Assets = assets

    def _build(self, lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT],
               *blocks: segment.Track) -> flow.Pipeline:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns: Assembled flow pipeline.
        """
        return flow.Pipeline.compose(self._engine.load(self._assets.project.source, lower, upper), *blocks)

    def train(self, lower: typing.Optional[etl.OrdinalT] = None, upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Return the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Training code.
        """
        path = self._build(lower or self._assets.tag.training.ordinal, upper,
                           self._assets.project.pipeline.expand()).train
        self._run(compiler.generate(path, self._assets.state(self._assets.tag.training.trigger())))

    def apply(self, lower: typing.Optional[etl.OrdinalT] = None, upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Return the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        path = self._build(lower, upper, self._assets.project.pipeline.expand()).apply
        self._run(compiler.generate(path, self._assets.state()))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[assembly.Symbol]) -> None:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            path: task graph to be executed.
        """
