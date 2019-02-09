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
    _SZIN = 1
    _SZOUT = 1

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
                return cls(task.Spec(actor, **params), **kwargs)
            return wrapper

        if actor:
            decorator = decorator(actor)
        return decorator

    def compose(self, left: segment.Composable) -> segment.Track:
        """Abstract composition implementation.

        Args:
            context: Worker context instance.
            left: Left side track builder.

        Returns: Composed track.
        """
        return self.apply(node.Worker(self._spec, self._SZIN, self._SZOUT), left.track())

    @abc.abstractmethod
    def apply(self, applier: node.Worker, left: segment.Track) -> segment.Track:
        """Apply functionality to be implemented by child.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns: Composed segment track.
        """


class Mapper(Simple):
    """Basic transformation operator with one input and one output port for each mode.
    """
    def apply(self, applier: node.Worker, left: segment.Track) -> segment.Track:
        """Mapper composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns: Composed segment track.
        """
        train_applier: node.Worker = applier.fork()
        if self._spec.actor.is_stateful():
            train_trainer: node.Worker = applier.fork()
            train_trainer.train(left.train.publisher, left.label.publisher)
        return left.extend(view.Path(applier), view.Path(train_applier))


class Consumer(Simple):
    """Basic operator with one input and one output port in apply mode and no output in train mode.
    """
    def apply(self, applier: node.Worker, left: segment.Track) -> segment.Track:
        """Consumer composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns: Composed segment track.
        """
        assert self._spec.actor.is_stateful(), 'Stateless actor invalid for a consumer'
        trainer: node.Worker = applier.fork()
        trainer.train(left.train.publisher, left.label.publisher)
        return left.extend(view.Path(applier))


class Labeler(Simple):
    """Basic label extraction operator.

    Provider actor is expected to have shape of (1, 2) where first output port is a train and second is label.
    """
    _SZOUT = 2

    def apply(self, applier: node.Worker, left: segment.Track) -> segment.Track:
        """Labeler composition implementation.

        Args:
            applier: Node factory to be used.
            left: Track of the left side flows.

        Returns: Composed segment track.
        """
        train: node.Future = node.Future()
        label: node.Future = node.Future()
        train[0].subscribe(applier[0])
        label[0].subscribe(applier[1])
        applier[0].subscribe(left.train.publisher)
        return left.use(train=left.train.extend(tail=train), label=left.train.extend(tail=label))
