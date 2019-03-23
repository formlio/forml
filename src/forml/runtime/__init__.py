"""
Execution layer.
"""
import abc
import datetime
import logging
import typing

import forml
from forml import etl, project
from forml.exec import meta
from forml.flow.graph import view

LOGGER = logging.getLogger(__name__)


class Registry(metaclass=abc.ABCMeta):
    """Parcel registry used by given runtime.
    """
    class Accessor:


    @classmethod
    @abc.abstractmethod
    def load(cls, name: typing.Optional[str] = None) -> meta.Parcel:
        """Load parcel by its name. If name isn't provided the most suitable (ie recent) parcel should be provided.

        Args:
            name: Optional name of parcel to be loaded.

        Returns: Parcel instance.
        """

    @classmethod
    @abc.abstractmethod
    def save(cls, parcel: meta.Parcel) -> None:
        """Persist the parcel in the registry.

        Args:
            parcel: Parcel instance to be persisted.
        """


class Runtime(typing.Generic[meta.StateT, etl.OrdinalT], metaclass=abc.ABCMeta):
    """Abstract base runtime class to be extended by particular runtime implementations.
    """
    def __init__(self, engine: etl.Engine[etl.OrdinalT], descriptor: project.Descriptor,
                 training: typing.Optional[meta.Training[meta.StateT, etl.OrdinalT]] = None,
                 tuning: typing.Optional[meta.Tuning] = None):
        self._engine: etl.Engine[etl.OrdinalT] = engine
        self.descriptor: project.Descriptor = descriptor
        self.training: typing.Optional[meta.Training[meta.StateT, etl.OrdinalT]] = training
        self.tuning: typing.Optional[meta.Tuning] = tuning

    @classmethod
    def restore(cls, engine: etl.Engine[etl.OrdinalT], parcel: typing.Optional[str] = None) -> 'Runtime':
        """Setup this runtime based on previously persisted parcel.

        Args:
            engine: ETL engine to be used.
            parcel: Optional parcel spec to be loaded (spec value to be interpretted by the Repository implementation).

        Returns: Runtime instance.
        """
        parcel: meta.Parcel = cls.Registry.load(parcel)
        return cls(engine, parcel.project, parcel.training, parcel.tuning)

    def persist(self) -> None:
        """Persist the runtime state as a new parcel into the registry.
        """
        self.Registry.save(meta.Parcel(self.descriptor, self.training, self.tuning))

    def train(self, lower: typing.Optional[etl.OrdinalT] = None, upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Perform the training.

        Args:
            lower: Optional lower ordinal bound to be used for trainset (defaults to last train max ordinal).
            upper: Optional upper ordinal bound to be used for trainset.
        """
        path: view.Path = self._engine.load(self.descriptor.source, lower or self.training.ordinal, upper)\
            .extend(*self.descriptor.pipeline.expand()).train
        timestamp = datetime.datetime.utcnow()
        states = self._run(path, meta.Binding.bind(path, self.training.states))
        self.training = meta.Training(timestamp, None, states)

    @abc.abstractmethod
    def _run(self, path: view.Path, states: meta.Binding[meta.StateT]) -> meta.Binding[meta.StateT]:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            path: task graph to be executed.

        Returns: Binding of new states potentially changed during execution.
        """
