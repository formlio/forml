from sklearn import model_selection, metrics

from forml.project import component
from forml.stdlib.operator.folding import evaluation

component.setup(evaluation.MergingScorer(
    crossvalidator=model_selection.StratifiedKFold(n_splits=3, shuffle=True, random_state=42),
    metric=metrics.log_loss))
