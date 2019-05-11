"""
Ensembling operators.
"""

import typing

from sklearn import model_selection

from forml.flow import task, pipeline
from forml.flow.graph import node
from forml.flow.pipeline import topology
from forml.stdlib.actor import frame
from forml.stdlib.operator import folding


class FullStacker(folding.Crossvalidated):
    """Stacked ensembling with all individual models kept for serving.
    """
    class Builder(folding.Crossvalidated.Builder):
        """Crossvalidation builder used as a folding context.
        """
        def __init__(self, head: pipeline.Segment,
                     stackers: typing.Dict[topology.Composable, node.Worker],
                     mergers: typing.Dict[topology.Composable, node.Worker],
                     trained: node.Worker, applied: node.Worker):
            self.head: pipeline.Segment = head
            self.stackers: typing.Dict[topology.Composable, node.Worker] = stackers
            self.mergers: typing.Dict[topology.Composable, node.Worker] = mergers
            self.trained: node.Worker = trained
            self.applied: node.Worker = applied

        def build(self) -> pipeline.Segment:
            """Builder finalize method.

            Returns: Crossvalidation pipeline segment.
            """
            return self.head.use(apply=self.head.apply.extend(tail=self.applied),
                                 train=self.head.train.extend(tail=self.trained))

    def __init__(self, bases: typing.Sequence[topology.Composable], crossvalidator: model_selection.BaseCrossValidator):
        super().__init__(crossvalidator)
        self.bases: typing.Sequence[topology.Composable] = bases
        self.merger: task.Spec = frame.Apply.spec(function=lambda df: df.mean(axis='columns'))

    def builder(self, head: pipeline.Segment, inner: pipeline.Segment) -> 'FullStacker.Builder':
        """Create a builder (folding context).

        Args:
            head: Head of the crossvalidation segment.
            inner: Exclusive instance of the inner composition.

        Returns: Builder instance.
        """
        trained: node.Worker = node.Worker(frame.Concat.spec(axis='columns'), len(self.bases), 1)
        applied: node.Worker = trained.fork()
        stack_forks: typing.Iterable[node.Worker] = node.Worker.fgen(frame.Concat.spec(axis='index'), self.nsplits, 1)
        bay_forks: typing.Iterable[node.Worker] = node.Worker.fgen(frame.Concat.spec(axis='columns'), self.nsplits, 1)
        merge_forks: typing.Iterable[node.Worker] = node.Worker.fgen(self.merger, 1, 1)
        stackers: typing.Dict[topology.Composable, node.Worker] = dict()
        mergers: typing.Dict[topology.Composable, node.Worker] = dict()
        for index, (base, stack, bay, merge) in enumerate(zip(self.bases, stack_forks, bay_forks, merge_forks)):
            stackers[base] = stack
            mergers[base] = bay
            merge[0].subscribe(bay[0])
            trained[index].subscribe(stackers[base][0])
            applied[index].subscribe(merge[0])

        return self.Builder(head, stackers, mergers, trained, applied)

    def fold(self, fold: int, builder: 'FullStacker.Builder',  # pylint: disable=arguments-differ
             pretrack: pipeline.Segment, features: node.Worker, labels: node.Worker) -> None:
        """Implement single fold ensembling.

        Args:
            fold: Fold index.
            builder: Composition builder (folding context).
            inner: Exclusive instance of the inner composition.
            features: Features splitter actor.
            labels: Labels splitter actor.
        """
        pretrack.train.subscribe(features[2 * fold])
        pretrack.label.subscribe(labels[2 * fold])
        pretrack.apply.subscribe(builder.head.apply.publisher)
        preapply = pretrack.apply.copy()
        preapply.subscribe(features[2 * fold + 1])
        for base in self.bases:
            basetrack = base.expand()
            basetrack.train.subscribe(pretrack.train.publisher)
            basetrack.label.subscribe(pretrack.label.publisher)
            basetrack.apply.subscribe(pretrack.apply.publisher)
            builder.mergers[base][fold].subscribe(basetrack.apply.publisher)
            baseapply = basetrack.apply.copy()
            baseapply.subscribe(preapply.publisher)
            builder.stackers[base][fold].subscribe(baseapply.publisher)
