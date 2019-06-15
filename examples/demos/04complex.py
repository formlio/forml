from sklearn import model_selection

from demos import *
from forml.stdlib.operator.folding import ensemble

FH_RFC = FeatureHasher(n_features=128) >> RFC(n_estimators=20, n_jobs=4, max_depth=3)
BIN_BAYES = Binarizer(threshold=0.63) >> Bayes(alpha=1.1)

STACK = ensemble.FullStacker(bases=(FH_RFC, BIN_BAYES),
                             crossvalidator=model_selection.StratifiedKFold(n_splits=2))

PIPELINE = SimpleImputer(strategy='mean') >> OneHotEncoder() >> STACK >> LR(max_iter=3, solver='lbfgs')

PROJECT = SOURCE.bind(PIPELINE)

PROJECT.launcher['graphviz'].train()
