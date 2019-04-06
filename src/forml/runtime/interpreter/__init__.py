

class Runner(typing.Generic[etl.OrdinalT], metaclass=abc.ABCMeta):
    """Abstract base runtime class to be extended by particular runtime implementations.
    """
    def __init__(self, engine: etl.Engine[etl.OrdinalT], symbols: symbol.Table):
        self._engine: etl.Engine[etl.OrdinalT] = engine
        self._objective: assembly.Assembly = symbols

    def train(self, lower: typing.Optional[etl.OrdinalT] = None, upper: typing.Optional[etl.OrdinalT] = None) -> None:
        """Perform the training.

        Args:
            lower: Optional lower ordinal bound to be used for trainset (defaults to last train max ordinal).
            upper: Optional upper ordinal bound to be used for trainset.
        """
        path: view.Path = self._engine.load(self._objective.descriptor.source, lower or self.training.ordinal, upper)\
            .extend(*self._objective.descriptor.pipeline.expand()).train
        timestamp = datetime.datetime.utcnow()
        states = self._run(path, resource.Binding.bind(path, self.training.states))
        self.training = resource.Training(timestamp, None, states)

    @abc.abstractmethod
    def _run(self, path: view.Path, states: resource.Binding) -> resource.Binding:
        """Actual run action to be implemented according to the specific runtime.

        Args:
            path: task graph to be executed.

        Returns: Binding of new states potentially changed during execution.
        """
