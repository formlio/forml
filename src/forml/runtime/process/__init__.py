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
        return pipeline.Composition(self._engine.load(self._assets.project.source, lower, upper),
                                    *(b.expand() for b in blocks))

    def train(self, lower: typing.Optional[etl.OrdinalT] = None,
              upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Run the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
        """
        path = self._build(lower or self._assets.tag.training.ordinal, upper,
                           self._assets.project.pipeline).train
        self._run(compiler.generate(path, self._assets.state(self._assets.tag.training.trigger())))

    def apply(self, lower: typing.Optional[etl.OrdinalT] = None,
              upper: typing.Optional[etl.OrdinalT] = None) -> typing.Any:
        """Run the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        path = self._build(lower, upper, self._assets.project.pipeline).apply
        return self._run(compiler.generate(path, self._assets.state()))

    def _evaluation(self, lower: typing.Optional[etl.OrdinalT] = None,
                    upper: typing.Optional[etl.OrdinalT] = None) -> pipeline.Segment:
        """Return the evaluation pipeline.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Evaluation pipeline.
        """
        return self._build(lower, upper, self._assets.project.pipeline >> self._assets.project.evaluation)

    def cvscore(self, lower: typing.Optional[etl.OrdinalT] = None,
                upper: typing.Optional[etl.OrdinalT] = None) -> typing.Any:
        """Run the crossvalidating evaluation.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Crossvalidate evaluation score.
        """
        return self._run(compiler.generate(self._evaluation(lower, upper).train, self._assets.state()))

    @abc.abstractmethod
    def _run(self, symbols: typing.Sequence[code.Symbol]) -> typing.Any:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            symbols: task graph to be executed.
        """
