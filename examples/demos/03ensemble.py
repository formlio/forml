from sklearn import model_selection

from demos import *
from forml.stdlib.operator.folding import ensemble

STACK = ensemble.FullStacker(bases=(RFC(max_depth=3), GBC(max_depth=3)),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2))

PIPELINE = SimpleImputer(strategy='mean') >> STACK >> LR(max_iter=3, solver='lbfgs')

PROJECT = SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
