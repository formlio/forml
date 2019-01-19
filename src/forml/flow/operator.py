import abc

from forml import flow
from forml.flow import task, segment, graph
from forml.flow.graph import node


class Singular(flow.Operator, metaclass=abc.ABCMeta):
    """Singular is a generic single actor operator.
    """
    def __init__(self, spec: task.Spec):
        self._spec: task.Spec = spec

    def compose(self, builder: segment.Builder) -> segment.Track:
        """Abstract composition implementation.

        Args:
            builder: Left side track builder.

        Returns: Composed track.
        """
        return self.apply(builder.track(), node.Factory(self._spec, 1, 1))

    @abc.abstractmethod
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Apply functionality to be implemented by child.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """


class Mapper(Singular):
    """Basic transformation operator with one input and one output port for each mode.
    """
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Mapper composition implementation.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """
        apply: node.Worker = worker.node()
        train_train: node.Worker = worker.node()
        train_apply: node.Worker = worker.node()
        train_train.train(left.train.publisher, left.label.publisher)
        return left.extend(graph.Path(apply), graph.Path(train_apply))


class Consumer(Singular):
    """Basic operator with one input and one output port in apply mode and no output in train mode.
    """
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Consumer composition implementation.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """
        apply: node.Worker = worker.node()
        train: node.Worker = worker.node()
        train.train(left.train.publisher, left.label.publisher)
        return left.extend(graph.Path(apply))


class Labeller(Singular):
    """Basic label extraction operator.
    """
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Labeller composition implementation.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """
        label: node.Worker = worker.node()
        return left.use(label=left.train.extend(graph.Path(label)))
