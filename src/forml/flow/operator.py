import abc
import inspect
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
    def decorate(cls, actor: typing.Optional[typing.Type[task.Actor]] = None, **kwargs):
        """Actor decorator for creating curried operator that get instantiated upon another (optionally parametrized)
        call.

        Args:
            actor: Decorated actor class.
            **kwargs: Optional operator kwargs.

        Returns: Curried operator.
        """
        assert bool(actor) ^ bool(kwargs), 'Unexpected positional argument provided together with keywords'

        def decorator(actor):
            """Decorating function.
            """
            assert actor and isinstance(actor, task.Actor), f'Invalid actor type {actor}'

            def operator(**params):
                """Curried operator.

                Args:
                    **params: Operator params.

                Returns: Operator instance.
                """
                return cls(task.Spec(actor, params), **kwargs)
            return operator

        if actor:  # used as paramless decorator
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


class Source(flow.Operator):
    """Basic source operator with optional label extraction.

    Label extractor is expected to be an actor with single input and two output ports - train and actual label.
    """
    def __init__(self, apply: task.Spec, train: task.Spec, label: typing.Optional[task.Spec] = None):
        self._apply: task.Spec = apply
        self._train: task.Spec = train
        self._label: task.Spec = label

    def compose(self, builder: segment.Builder) -> segment.Track:
        """Compose the source segment track.

        Returns: Source segment track.
        """
        assert isinstance(builder, segment.Origin), 'Source not origin'
        apply: view.Path = view.Path(node.Factory(self._apply, 0, 1).node())
        train: view.Path = view.Path(node.Factory(self._train, 0, 1).node())
        label: typing.Optional[view.Path] = None
        if self._label:
            train_tail = node.Future()
            label_tail = node.Future()
            extract = node.Factory(self._label, 1, 2).node()
            extract[0].subscribe(train[0])
            train_tail[0].subscribe(extract[0])
            label_tail[0].subscribe(extract[1])
            train = train.extend(tail=train_tail)
            label = train.extend(tail=label_tail)
        return segment.Track(apply, train, label)
