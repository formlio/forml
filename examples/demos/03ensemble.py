from sklearn import model_selection

import demos
from forml.stdlib.operator.folding import ensemble

STACK = ensemble.FullStacker(bases=(demos.RFC(max_depth=3), demos.GBC(max_depth=3)),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2))

PIPELINE = demos.SimpleImputer(strategy='mean') >> STACK >> demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
