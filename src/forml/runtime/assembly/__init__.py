import abc
import collections
import datetime
import logging
import time
import typing

from forml import etl
from forml.flow import segment
from forml.flow.graph import view, node as grnode
from forml.runtime import asset
from forml.runtime.assembly import instruction as instmod, symbol as symbmod
from forml.runtime.asset import directory, state

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

    def _tag(self, **modkw) -> directory.Generation.Tag:
        try:
            return self._assets.tag._replace(**modkw)
        except directory.Level.Listing.Empty:
            pass
        return directory.Generation.Tag(**modkw)

    def _link(self, lower: typing.Optional[etl.OrdinalT], upper: typing.Optional[etl.OrdinalT],
              *blocks: segment.Track) -> segment.Track:
        linked = self._engine.load(self._assets.project.source, lower, upper)
        for track in blocks:
            linked = linked.extend(track.apply, track.train, track.label)
        return linked

    def _generate(self, path: view.Path, assets: state.Manager) -> typing.Sequence[Symbol]:
        table = symbmod.Table(assets)
        path.accept(table)
        return tuple(table)

    def training(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[Symbol]:
        """Return the training code.

        Returns: Training code.
        """
        tag = self._tag(training=directory.Generation.Tag.Training(timestamp=datetime.datetime.utcnow()))
        path = self._link(lower or tag.training.ordinal, upper, self._assets.project.pipeline.expand()).train
        return self._generate(path, self._assets.state(tag))

    def applying(self, lower: typing.Optional[etl.OrdinalT] = None,
                 upper: typing.Optional[etl.OrdinalT] = None) -> typing.Sequence[Symbol]:
        """Return the applying code.

        Returns: Applying code.
        """
        path = self._link(lower, upper, self._assets.project.pipeline.expand()).apply
        return self._generate(path, self._assets.state())
