"""
Set of generic operator skeletons that can be simply used as wrappers about relevant actors.
"""
import abc
import typing

from forml import flow
from forml.flow import task, segment
from forml.flow.graph import node, view


class Simple(flow.Operator, metaclass=abc.ABCMeta):
    """Simple is a generic single actor operator.
    """
    def __init__(self, spec: task.Spec):
        self._spec: task.Spec = spec

    @classmethod
    def operator(cls, actor: typing.Optional[typing.Type[task.Actor]] = None, **kwargs):
        """Actor decorator for creating curried operator that get instantiated upon another (optionally parametrized)
        call.

        Args:
            actor: Decorated actor class.
            **kwargs: Optional operator kwargs.

        Returns: Curried operator.
        """
        def decorator(actor):
            """Decorating function.
            """
            def wrapper(**params):
                """Curried operator.

                Args:
                    **params: Operator params.

                Returns: Operator instance.
                """
                return cls(task.Spec(actor, params), **kwargs)
            return wrapper

        if actor:
            decorator = decorator(actor)
        return decorator

    def compose(self, left: segment.Builder) -> segment.Track:
        """Abstract composition implementation.

        Args:
            left: Left side track builder.

        Returns: Composed track.
        """
        return self.apply(left.track(), node.Factory(self._spec, 1, 1))

    @abc.abstractmethod
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Apply functionality to be implemented by child.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """


class Mapper(Simple):
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
        return left.extend(view.Path(apply), view.Path(train_apply))


class Consumer(Simple):
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
        return left.extend(view.Path(apply))


class Labeler(Simple):
    """Basic label extraction operator.

    Actual train path is left intact.
    """
    def apply(self, left: segment.Track, worker: node.Factory) -> segment.Track:
        """Labeler composition implementation.

        Args:
            left: Track of the left side flows.
            worker: Node factory to be used.

        Returns: Composed segment track.
        """
        label: node.Worker = worker.node()
        return left.use(label=left.train.extend(view.Path(label)))
