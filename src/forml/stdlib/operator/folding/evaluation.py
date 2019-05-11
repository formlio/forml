"""
Folding base model performance evaluation operators.
"""
import statistics
import typing

import pandas
from sklearn import model_selection

from forml.flow import task, pipeline
from forml.flow.graph import node, port
from forml.stdlib.actor import wrapped
from forml.stdlib.operator import folding


class MergingScorer(folding.Crossvalidated):
    """Evaluation scorer based on merging partial crossvalidation scoring results.
    """

    class Builder(folding.Crossvalidated.Builder):
        """Crossvalidation builder used as a folding context.
        """
        def __init__(self, outer: pipeline.Segment, merger: node.Worker):
            self.outer: pipeline.Segment = outer
            self.merger: node.Worker = merger

        def build(self) -> pipeline.Segment:
            """Builder finalize method.

            Returns: Crossvalidation pipeline segment.
            """
            return self.outer.use(train=self.outer.train.extend(tail=self.merger))

    def __init__(self, crossvalidator: model_selection.BaseCrossValidator,
                 metric: typing.Callable[[pandas.Series, pandas.Series], float],
                 merger: typing.Callable[[float], float] = lambda *m: statistics.mean(m)):
        super().__init__(crossvalidator)
        self.metric: task.Spec = wrapped.Function.Actor.spec(function=metric)
        self.merger: task.Spec = wrapped.Function.Actor.spec(function=merger)

    def score(self, ytrue: port.Publishable, ypred: port.Publishable) -> node.Atomic:
        """Metric scoring routing.

        Args:
            ytrue: Publisher of the tru labels.
            ypred: Publisher of the predicted values.

        Returns: Scoring worker node.
        """
        scorer = node.Worker(self.metric, 2, 1)
        scorer[0].subscribe(ytrue)
        scorer[1].subscribe(ypred)
        return scorer

    def builder(self, head: pipeline.Segment, inner: pipeline.Segment) -> 'MergingScorer.Builder':
        """Create a builder (folding context).

        Args:
            head: Head of the crossvalidation segment.
            inner: Exclusive instance of the inner composition.

        Returns: Builder instance.
        """
        # TODO: apply path based on trainset is confusing compiler
        # inner.apply.subscribe(head.train.publisher)
        # scorer = self.score(head.label.publisher, inner.apply.publisher)
        merger = node.Worker(self.merger, self.nsplits, 1)
        # return self.Builder(head.use(apply=head.train.extend(tail=scorer)), merger)
        return self.Builder(head, merger)

    def fold(self, fold: int, builder: 'MergingScorer.Builder', inner: pipeline.Segment,
             features: node.Worker, labels: node.Worker) -> None:
        """Implement single fold ensembling.

        Args:
            fold: Fold index.
            builder: Composition builder (folding context).
            inner: Exclusive instance of the inner composition.
            features: Features splitter actor.
            labels: Labels splitter actor.
        """
        inner.apply.subscribe(features[2 * fold + 1])
        inner.train.subscribe(features[2 * fold])
        inner.label.subscribe(labels[2 * fold])
        builder.merger[fold].subscribe(self.score(labels[2 * fold + 1], inner.apply.publisher)[0])
