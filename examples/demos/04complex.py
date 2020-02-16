from sklearn import model_selection

import demos
from forml.stdlib.operator.folding import ensemble

FH_RFC = demos.FeatureHasher(n_features=128) >> demos.RFC(n_estimators=20, n_jobs=4, max_depth=3)
BIN_BAYES = demos.Binarizer(threshold=0.63) >> demos.Bayes(alpha=1.1)

STACK = ensemble.FullStacker(bases=(FH_RFC, BIN_BAYES),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2))

PIPELINE = demos.SimpleImputer(strategy='mean') >> \
    demos.OneHotEncoder() >> \
    STACK >> \
    demos.LR(max_iter=3, solver='lbfgs')

PROJECT = demos.SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
