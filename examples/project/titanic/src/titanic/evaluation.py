"""
Titanic evaluation definition.

This is one of the main _formal_ forml components (along with `source` and `evaluation`) that's being looked up by
the forml loader.
"""

from sklearn import model_selection, metrics

from forml.project import component
from forml.stdlib.operator.folding import evaluation

# Typical method of providing component implementation using `component.setup()`. Choosing the `MergingScorer` operator
# to implement classical crossvalidated metric scoring
component.setup(evaluation.MergingScorer(
    crossvalidator=model_selection.StratifiedKFold(n_splits=3, shuffle=True, random_state=42),
    metric=metrics.log_loss))
