"""
Ensembling operators.
"""

import typing

from forml import flow
from forml.flow import segment, task
from forml.flow.graph import view, node


class Stack(flow.Operator):
    """Crossvalidating stacked ensembling transformation.
    """

    def __init__(self, bases: typing.Sequence[segment.Composable], folds: int = 2):
        self._bases: typing.Sequence[segment.Composable] = bases
        self._folds: int = folds
        self._splitter: task.Spec = task.Spec('splitter', folds=self._folds)
        self._merger: task.Spec = task.Spec('merger', szin=self._folds)
        self._appender: task.Spec = task.Spec('appender', szin=self._folds)
        self._averager: task.Spec = task.Spec('averager', szin=self._folds)

    def compose(self, left: segment.Composable) -> segment.Track:
        """Ensemble composition.

        Args:
            left: left segment.

        Returns: Composed segment track.
        """
        apply: node.Future = node.Future(1, 1)
        train: node.Future = node.Future(1, 1)
        label: node.Future = node.Future(1, 1)
        splitter: node.Worker.Instance = node.Worker.Instance(self._splitter, 1, 2 * self._folds)
        train_splitter: node.Worker = splitter.node()
        train_splitter.train(train[0], label[0])
        features_splitter: node.Worker = splitter.node()
        label_splitter: node.Worker = splitter.node()
        features_splitter[0].subscribe(train[0])
        label_splitter[0].subscribe(label[0])
        merger: node.Worker.Instance = node.Worker.Instance(self._merger, len(self._bases), 1)
        train_merger: node.Worker = merger.node()
        apply_merger: node.Worker = merger.node()
        appender: node.Worker.Instance = node.Worker.Instance(self._appender, self._folds, 1)
        averager: node.Worker.Instance = node.Worker.Instance(self._averager, self._folds, 1)
        train_appender: typing.Dict[segment.Composable, node.Worker] = dict()
        apply_averager: typing.Dict[segment.Composable, node.Worker] = dict()
        for index, base in enumerate(self._bases):
            train_appender[base] = appender.node()
            apply_averager[base] = averager.node()
            train_merger[index].subscribe(train_appender[base][0])
            apply_merger[index].subscribe(apply_averager[base][0])
        for fold in range(self._folds):
            pretrack: segment.Track = left.track()
            pretrack.train.subscribe(features_splitter[fold])
            pretrack.label.subscribe(label_splitter[fold])
            pretrack.apply.subscribe(apply[0])
            preapply = pretrack.apply.copy()
            preapply.subscribe(features_splitter[fold + self._folds])
            for base in self._bases:
                basetrack = base.track()
                basetrack.train.subscribe(pretrack.train.publisher)
                basetrack.label.subscribe(pretrack.label.publisher)
                basetrack.apply.subscribe(pretrack.apply.publisher)
                apply_averager[base][fold].subscribe(basetrack.apply.publisher)
                baseapply = basetrack.apply.copy()
                baseapply.subscribe(preapply.publisher)
                train_appender[base][fold].subscribe(baseapply.publisher)

        return segment.Track(view.Path(apply, apply_merger), view.Path(train, train_merger), view.Path(label, label))
