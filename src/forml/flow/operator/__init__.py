"""Flow operators code.
"""
import typing

from forml import flow
from forml.flow import task, segment
from forml.flow.graph import node, view


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
