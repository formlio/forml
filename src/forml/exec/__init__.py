import abc
import datetime
import logging
import typing

import forml
from forml import etl
from forml.exec import meta
from forml.flow.graph import view, node


LOGGER = logging.getLogger(__name__)


class Runtime(typing.Generic[meta.StateT, etl.SelectT, etl.OrdinalT], metaclass=abc.ABCMeta):
    class Instance(metaclass=abc.ABCMeta):
        @abc.abstractmethod
        def load(self) -> meta.Package:
            ...

        @classmethod
        @abc.abstractmethod
        def save(cls, package: meta.Package) -> 'Runtime.Instance':
            ...

    def __init__(self, engine: etl.Engine[etl.SelectT, etl.OrdinalT], instance: Instance):
        self._engine: etl.Engine[etl.OrdinalT] = engine
        package = instance.load()
        self.project: forml.Project = package.project
        self.training: typing.Optional[meta.Training[meta.StateT, etl.OrdinalT]] = package.training
        self.tuning: typing.Optional[meta.Tuning] = package.tuning

    def train(self, lower: typing.Optional[etl.OrdinalT] = None, upper: typing.Optional[etl.OrdinalT] = None) -> None:
        path = self._engine.load(self.project.source, lower or self.training.ordinal, upper)\
            .extend(*self.project.pipeline.track()).train
        nodes: typing.Sequence[node.Worker] = ...
        states = meta.Registry(nodes, self.training.states)
        timestamp = datetime.datetime.utcnow()
        states = self._run(path, states)
        self.training = meta.Training(timestamp, None, states)

    def persist(self) -> 'Runtime.Instance':
        return self.Instance.save(meta.Package(self.project, self.training, self.tuning))

    @abc.abstractmethod
    def _run(self, path: view.Path, states: meta.Registry[meta.StateT]) -> meta.Registry[meta.StateT]:
        """Runtime is supposed to manage output itself.

        Args:
            path:

        Returns:

        """
