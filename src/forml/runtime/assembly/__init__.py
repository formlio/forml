import abc
import collections
import logging
import time
import typing

from forml import etl
from forml.flow import segment
from forml.runtime import asset
from forml.runtime.assembly import instruction as instmod, compiler

LOGGER = logging.getLogger(__name__)


class Instruction(metaclass=abc.ABCMeta):
    """Callable part of an assembly symbol that's responsible for implementing the processing activity.
    """
    @abc.abstractmethod
    def execute(self, *args: typing.Any) -> typing.Any:
        """Instruction functionality.

        Args:
            *args: Sequence of input arguments.

        Returns: Instruction result.
        """

    def __str__(self):
        return f'{self.__class__.__name__}[{id(self)}]'

    def __call__(self, *args: typing.Any) -> typing.Any:
        LOGGER.debug('%s invoked (%d args)', self, len(args))
        start = time.time()
        result = self.execute(*args)
        LOGGER.debug('%s completed (%.2fms)', self, (time.time() - start) * 1000)
        return result


class Symbol(collections.namedtuple('Symbol', 'instruction, arguments')):
    """Main entity of the assembled code.
    """
    def __new__(cls, instruction: Instruction, arguments: typing.Optional[typing.Sequence[Instruction]] = None):
        if arguments is None:
            arguments = []
        assert all(arguments), 'All arguments required'
        return super().__new__(cls, instruction, tuple(arguments))

    def __str__(self):
        return f'{self.instruction}{self.arguments}'


class Linker:
    """Linker is doing the hard work of assembling the low-level task graph based on the selected mode combining with
    particular ETL engine and possibly any persistence steps.
    """
    def __init__(self, engine: etl.Engine, assets: asset.Manager):
        self._engine: etl.Engine = engine
        self._assets: asset.Manager = assets

    @classmethod
    def load(cls, engine: etl.Engine, registry: asset.Registry, project: str, lineage: typing.Optional[int] = None,
             generation: typing.Optional[int] = None) -> 'Linker':
        """Create the linker instance based on project loaded from a registry.

        Args:
            engine: ETL engine to use.

        Returns: Linker instance.
        """
        return cls(engine, asset.Manager(registry, project, lineage, generation))

    def _assemble(self, lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT],
                  *blocks: segment.Track) -> segment.Track:
        """Assemble the chain of blocks with the mandatory ETL cycle.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.
            *blocks: Additional block to assemble.

        Returns: Assembled flow track.
        """
        linked = self._engine.load(self._assets.project.source, lower, upper)
        for track in blocks:
            linked = linked.extend(track.apply, track.train, track.label)
        return linked

    def training(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[Symbol]:
        """Return the training code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Training code.
        """
        path = self._assemble(lower or self._assets.tag.training.ordinal, upper,
                              self._assets.project.pipeline.expand()).train
        return compiler.generate(path, self._assets.state(self._assets.tag.training.trigger()))

    def applying(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[Symbol]:
        """Return the applying code.

        Args:
            lower: Ordinal value as the lower bound for the ETL cycle.
            upper:  Ordinal value as the upper bound for the ETL cycle.

        Returns: Applying code.
        """
        path = self._assemble(lower, upper, self._assets.project.pipeline.expand()).apply
        return compiler.generate(path, self._assets.state())
