"""
Ensembling operators.
"""

import typing

from sklearn import model_selection

from forml import flow
from forml.flow import segment, task
from forml.flow.graph import view, node
from forml.stdlib.actor import frame


class Stack(flow.Operator):
    """Crossvalidating stacked ensembling transformation.
    """

    def __init__(self, bases: typing.Sequence[segment.Composable], crossvalidator: model_selection.BaseCrossValidator):
        self._bases: typing.Sequence[segment.Composable] = bases
        self._folds: int = crossvalidator.get_n_splits()
        self._splitter: task.Spec = frame.TrainTestSplit.spec(crossvalidator=crossvalidator)
        self._merger: task.Spec = frame.Merge.spec()
        self._appender: task.Spec = frame.Append.spec()
        self._averager: task.Spec = frame.Apply.spec(method=lambda df: df.mean(axis='columns'))

    def compose(self, left: segment.Composable) -> segment.Track:
        """Ensemble composition.

        Args:
            left: left segment.
            context: Worker context instance.

        Returns: Composed segment track.
        """
        apply: node.Future = node.Future(1, 1)
        train: node.Future = node.Future(1, 1)
        label: node.Future = node.Future(1, 1)
        splitter_trainer: node.Worker = node.Worker(self._splitter, 1, 2 * self._folds)
        splitter_trainer.train(train[0], label[0])
        features_splitter: node.Worker = splitter_trainer.fork()
        label_splitter: node.Worker = splitter_trainer.fork()
        features_splitter[0].subscribe(train[0])
        label_splitter[0].subscribe(label[0])
        train_merger: node.Worker = node.Worker(self._merger, len(self._bases), 1)
        apply_merger: node.Worker = train_merger.fork()
        appender_forks: typing.Iterable[node.Worker] = node.Worker.fgen(self._appender, self._folds, 1)
        averager_forks: typing.Iterable[node.Worker] = node.Worker.fgen(self._averager, self._folds, 1)
        train_appender: typing.Dict[segment.Composable, node.Worker] = dict()
        apply_averager: typing.Dict[segment.Composable, node.Worker] = dict()
        for index, (base, appender, averager) in enumerate(zip(self._bases, appender_forks, averager_forks)):
            train_appender[base] = appender
            apply_averager[base] = averager
            train_merger[index].subscribe(train_appender[base][0])
            apply_merger[index].subscribe(apply_averager[base][0])
        for fold in range(self._folds):
            pretrack: segment.Track = left.expand()
            pretrack.train.subscribe(features_splitter[2 * fold])
            pretrack.label.subscribe(label_splitter[2 * fold])
            pretrack.apply.subscribe(apply[0])
            preapply = pretrack.apply.copy()
            preapply.subscribe(features_splitter[2 * fold + 1])
            for base in self._bases:
                basetrack = base.expand()
                basetrack.train.subscribe(pretrack.train.publisher)
                basetrack.label.subscribe(pretrack.label.publisher)
                basetrack.apply.subscribe(pretrack.apply.publisher)
                apply_averager[base][fold].subscribe(basetrack.apply.publisher)
                baseapply = basetrack.apply.copy()
                baseapply.subscribe(preapply.publisher)
                train_appender[base][fold].subscribe(baseapply.publisher)

        return segment.Track(view.Path(apply, apply_merger), view.Path(train, train_merger), view.Path(label, label))
